/*-------------------------------------------------------------------------
*
* ebi_tree.c
*
* EBI Tree Implementation
*
*
* Copyright (C) 2021 Scalable Computing Systems Lab.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this program.  If not, see <https://www.gnu.org/licenses/>.
*
*
* IDENTIFICATION
*	  src/backend/storage/ebi_tree/ebi_tree.c
*
*-------------------------------------------------------------------------
*/
#ifdef DIVA
#include "postgres.h"

#include "storage/lwlock.h"
#include "postmaster/ebi_tree_process.h"
#include "storage/ebi_tree.h"
#include "storage/ebi_tree_buf.h"
#include "storage/ebi_tree_utils.h"
#include "storage/ebi_sub_buf.h"
#include "access/heapam.h"
#ifdef LOCATOR
#include "pgstat.h"
#include "storage/readbuf.h"
#include "utils/snapmgr.h"
#endif /* LOCATOR */
#include "storage/procarray.h"
#include "utils/dynahash.h"
#include "utils/snapmgr.h"
#include "access/xact.h"

#include "storage/proc.h"

#ifdef DIVA_PRINT
#include "optimizer/cost.h"
#endif

/* Helper macros for segment management */
#define EBI_TREE_INVALID_SEG_ID ((EbiTreeSegmentId)(0))
#define EBI_TREE_INVALID_VERSION_OFFSET ((EbiTreeVersionOffset)(-1))
#define EBI_TREE_SEG_ID_MASK (0xFFFFFFFF00000000ULL)
#define EBI_TREE_SEG_OFFSET_MASK (0x00000000FFFFFFFFULL)
#define EBI_TREE_VERSION_OFFSET_TO_SEG_ID(version_offset) \
	((version_offset & EBI_TREE_SEG_ID_MASK) >> 32)
#define EBI_TREE_VERSION_OFFSET_TO_SEG_OFFSET(version_offset) \
	(version_offset & EBI_TREE_SEG_OFFSET_MASK)
#define EBI_TREE_SEG_TO_VERSION_OFFSET(seg_id, seg_offset) \
	((((uint64)(seg_id)) << 32) | seg_offset)

/* Helper macros for statistics */
#define EBI_VERSION_CHUNK_SIZE (1000)
#define EBI_AVG_STAT_WEIGHT (0.999)

/* src/include/postmaster/ebitree_process.h */
dsa_area* ebitree_dsa_area;

/* Prototypes for private functions */

/* Allocation */
static dsa_pointer EbiSubCreateNodeWithHeight(dsa_area* area, uint32 height);

static dsa_pointer EbiCreateNode(dsa_area* area);
static dsa_pointer EbiFindInsertionTargetNode(EbiTree ebitree);
static void EbiUnlinkNode(EbiTree ebitree,
						  dsa_pointer dsa_node,
						  EbiSpscQueue delete_queue);
static void EbiUnlinkFromParent(EbiNode node);
static void EbiPushToGarbageQueue(EbiSpscQueue delete_queue,
								  EbiNode node,
								  dsa_pointer dsa_node);
static void EbiCompactNode(EbiTree ebitree, dsa_pointer dsa_node);
static void EbiDeleteTreeRecursive(EbiNode node, dsa_pointer dsa_node);

static void EbiLinkProxy(dsa_pointer dsa_proxy, dsa_pointer dsa_proxy_target);

/* Reference Counting */
static uint32 EbiIncreaseRefCountInternal(EbiNode node);
static uint32 EbiDecreaseRefCountInternal(EbiNode node);

static void EbiSetLeftBoundary(EbiNode node, Snapshot snapshot, bool is_copied);
static void EbiSetRightBoundary(EbiNode node, Snapshot snapshot, bool is_copied);
static dsa_pointer EbiSetRightBoundaryRecursive(dsa_pointer dsa_node,
												Snapshot snapshot);

/* External function's internal implementation */
static EbiNode EbiSiftInternal(TransactionId xmin, TransactionId xmax);
static bool EbiSegIsAliveInternal(dsa_pointer dsa_ebitree,
								  EbiTreeSegmentId seg_id);

/* DSA based version of CopySnapshot in snapmgr.c */
static dsa_pointer EbiDsaCopySnapshot(Snapshot snapshot, bool is_copied);

/* Utility */
static bool EbiHasParent(EbiNode node);
static bool EbiHasLeftChild(EbiNode node);
static bool EbiIsLeftChild(EbiNode node);

static dsa_pointer EbiSibling(EbiNode node);

static bool EbiOverlaps(EbiNode node, TransactionId xmin, TransactionId xmax);

/* Statisitcs */
static void EbiUpdateAverageVersionLifetime(TransactionId xmin,
											TransactionId xmax);
static void EbiUpdateTimespec(void);

/* EbiSubNode */
static void EbiDeleteSubNodes(EbiNode node);
static EbiSubNode EbiGetSubNode(EbiNode node, uint8 idx);
static EbiSubNode EbiCreateSubNode(EbiNode node, Oid rel_id);
static EbiSubNode EbiCreateSubNodeInternal(EbiNode node, Oid rel_id);
extern TupleTableSlot *
ExecStoreBufferHeapTuple_for_ebi(HeapTuple tuple,
								 TupleTableSlot *slot,
								 Buffer buffer);

dsa_pointer
EbiInitTree(dsa_area* area)
{
	dsa_pointer dsa_ebitree, dsa_sentinel;
	EbiTree ebitree;

	dsa_ebitree =
		dsa_allocate_extended(area, sizeof(EbiTreeData), DSA_ALLOC_ZERO);
	ebitree = EbiConvertToTree(area, dsa_ebitree);

	dsa_sentinel = EbiCreateNode(area);

	ebitree->root = dsa_sentinel;
	ebitree->recent_node = dsa_sentinel;

	return dsa_ebitree;
}

static dsa_pointer
EbiCreateNode(dsa_area* area)
{
	return EbiSubCreateNodeWithHeight(area, 0);
}

void
EbiDeleteTree(dsa_pointer dsa_ebitree)
{
	EbiTree ebitree;
	dsa_pointer dsa_root;
	EbiNode root;

	if (!DsaPointerIsValid(dsa_ebitree))
	{
		return;
	}

	ebitree = EbiConvertToTree(ebitree_dsa_area, dsa_ebitree);

	dsa_root = ebitree->root;
	root = EbiConvertToNode(ebitree_dsa_area, dsa_root);

#ifdef LOCATOR
	EbiTreeShmem->isShutdown = true;
#endif /* LOCATOR */

	/* Delete remaining nodes */
	EbiDeleteTreeRecursive(root, dsa_root);

	dsa_free(ebitree_dsa_area, dsa_ebitree);
}

static void
EbiDeleteTreeRecursive(EbiNode node, dsa_pointer dsa_node)
{
	if (node == NULL)
	{
		return;
	}

	EbiDeleteTreeRecursive(EbiConvertToNode(ebitree_dsa_area, node->left),
											node->left);
	EbiDeleteTreeRecursive(EbiConvertToNode(ebitree_dsa_area, node->right),
											node->right);
	EbiDeleteNode(node, dsa_node);
}

void
EbiInsertNode(dsa_pointer dsa_ebitree)
{
	EbiTree ebitree;

	dsa_pointer dsa_target;
	dsa_pointer dsa_new_parent;
	dsa_pointer dsa_new_leaf;

	EbiNode target;
	EbiNode new_parent;
	EbiNode new_leaf;
	Snapshot snap;

	ebitree = EbiConvertToTree(ebitree_dsa_area, dsa_ebitree);

	/*
	 * Find the target ebi_node to perform insertion, make a new parent for the
	 * target ebi_node and set its right to the newly inserted node
	 *
	 *    new_parent
	 *	   /      \
	 *  target  new_leaf
	 *
	 */
	dsa_target = EbiFindInsertionTargetNode(ebitree);
	target = EbiConvertToNode(ebitree_dsa_area, dsa_target);

	dsa_new_parent =
		EbiSubCreateNodeWithHeight(ebitree_dsa_area, target->height + 1);

	new_parent = EbiConvertToNode(ebitree_dsa_area, dsa_new_parent);

	dsa_new_leaf = EbiCreateNode(ebitree_dsa_area);
	new_leaf = EbiConvertToNode(ebitree_dsa_area, dsa_new_leaf);

	// Set the left bound of the new parent to the left child's left bound.
	snap = (Snapshot)dsa_get_address(ebitree_dsa_area, target->left_boundary);
	EbiSetLeftBoundary(new_parent, snap, true);

	/*
	 * Connect the original parent as the new parent's parent.
	 * In the figure below, connecting nodes 'a' and 'f'.
	 * (e = target, f = new_parent, g = new_leaf)
	 *
	 *	    a	         a
	 *	   / \	        / \
	 *    b   \	  ->   b   f
	 *   / \   \	  / \ / \
	 *  c   d   e	 c 	d e  g
	 *
	 */
	if (EbiHasParent(target))
	{
		new_parent->parent = target->parent;
	}
	// f->e, f->g
	new_parent->left = dsa_target;
	new_parent->right = dsa_new_leaf;

#ifdef PRINT_EBI_INFO
	new_parent->left_most = target->left_most;
#endif
	/*
	 * At this point, the new nodes('f' and 'g') are not visible
	 * since they are not connected to the original tree.
	 */
	pg_memory_barrier();

	// a->f
	if (EbiHasParent(target))
	{
		EbiNode tmp;
		tmp = EbiConvertToNode(ebitree_dsa_area, target->parent);
		tmp->right = dsa_new_parent;
	}
	// e->f, g->f
	target->parent = dsa_new_parent;
	new_leaf->parent = dsa_new_parent;

	// If the target node is root, the root is changed to the new parent.
	if (target == EbiConvertToNode(ebitree_dsa_area, ebitree->root))
	{
		ebitree->root = dsa_new_parent;
	}

	pg_memory_barrier();

	// Change the last leaf node to the new right node.
	ebitree->recent_node = dsa_new_leaf;
}

static dsa_pointer
EbiFindInsertionTargetNode(EbiTree ebitree)
{
	dsa_pointer dsa_tmp;
	dsa_pointer dsa_parent;

	EbiNode tmp;
	EbiNode parent;
	EbiNode left;
	EbiNode right;

#ifdef PRINT_EBI_INFO
	uint32 right_most;
#endif
	dsa_tmp = ebitree->recent_node;
	tmp = EbiConvertToNode(ebitree_dsa_area, dsa_tmp);

#ifdef PRINT_EBI_INFO
	right_most = tmp->left_most;
#endif
	dsa_parent = tmp->parent;
	parent = EbiConvertToNode(ebitree_dsa_area, dsa_parent);

	while (parent != NULL)
	{
		left = EbiConvertToNode(ebitree_dsa_area, parent->left);
		right = EbiConvertToNode(ebitree_dsa_area, parent->right);

#ifdef PRINT_EBI_INFO
		if (parent->seg_id - parent->left_most >
				right_most - parent->seg_id)
#else
		if (left->height > right->height)
#endif
		{
			// Unbalanced, found target node.
			break;
		}
		else
		{
			dsa_tmp = dsa_parent;
			tmp = EbiConvertToNode(ebitree_dsa_area, dsa_tmp);
			dsa_parent = tmp->parent;
			parent = EbiConvertToNode(ebitree_dsa_area, dsa_parent);
		}
	}

	return dsa_tmp;
}

/**
 * Reference Counting
 */

dsa_pointer
EbiIncreaseRefCount(Snapshot snapshot)
{
	EbiTree tree;
	dsa_pointer dsa_recent_node, dsa_sibling, dsa_prev_node;
	EbiNode recent_node;
	EbiNode sibling;
	uint32 refcnt;

	tree = EbiConvertToTree(ebitree_dsa_area, EbiTreeShmem->ebitree);
	dsa_recent_node = tree->recent_node;
	recent_node = EbiConvertToNode(ebitree_dsa_area, dsa_recent_node);

	refcnt = EbiIncreaseRefCountInternal(recent_node);

	// The first one to enter the current node should set the boundary.
	if (refcnt == 1)
	{
		/* For left and right boundary's seq */
		snapshot->seq = ++EbiTreeShmem->seq;

		// The next epoch's opening transaction will decrease ref count twice.
		refcnt = EbiIncreaseRefCountInternal(recent_node);

		dsa_sibling = EbiSibling(recent_node);
		sibling = EbiConvertToNode(ebitree_dsa_area, dsa_sibling);

		EbiSetLeftBoundary(recent_node, snapshot, false);

		// When the initial root node stands alone, sibling could be NULL.
		if (sibling != NULL)
		{
			EbiSetRightBoundary(sibling, snapshot, false);
			pg_memory_barrier();
			dsa_prev_node = EbiSetRightBoundaryRecursive(dsa_sibling, snapshot);

			// May delete the last recent node if there's no presence of any
			// xacts.
			EbiDecreaseRefCount(dsa_prev_node);
		}

		/* Stats */
		EbiTreeShmem->max_xid = EbiGetMaxTransactionId();
		EbiUpdateTimespec();
	}

	/* Safe since ProcArrayLock is held */
	snapshot->seq = ++EbiTreeShmem->seq;

	return dsa_recent_node;
}

static uint32
EbiIncreaseRefCountInternal(EbiNode node)
{
	uint32 ret;
	ret = pg_atomic_add_fetch_u32(&node->refcnt, 1);
	return ret;
}

static void
EbiSetLeftBoundary(EbiNode node, Snapshot snapshot, bool is_copied)
{
	node->left_boundary = EbiDsaCopySnapshot(snapshot, is_copied);
}

static void
EbiSetRightBoundary(EbiNode node, Snapshot snapshot, bool is_copied)
{
	node->right_boundary = EbiDsaCopySnapshot(snapshot, is_copied);
}

/* DSA version of CopySnapshot in snapmgr.c */
static dsa_pointer
EbiDsaCopySnapshot(Snapshot snapshot, bool is_copied)
{
	dsa_pointer dsa_newsnap;
	Snapshot newsnap;
	Size subxipoff;
	Size size;
	TransactionId *snapxip;
	TransactionId *snapsubxip;

	Assert(snapshot != InvalidSnapshot);

	size = subxipoff =
		sizeof(SnapshotData) + snapshot->xcnt * sizeof(TransactionId);
	if (snapshot->subxcnt > 0)
		size += snapshot->subxcnt * sizeof(TransactionId);

	/* Allocate DSA */
	dsa_newsnap = dsa_allocate_extended(ebitree_dsa_area, size, DSA_ALLOC_ZERO);
	newsnap = (Snapshot)dsa_get_address(ebitree_dsa_area, dsa_newsnap);
	memcpy(newsnap, snapshot, sizeof(SnapshotData));

	newsnap->regd_count = 0;
	newsnap->active_count = 0;
	newsnap->copied = true;

	if (is_copied)
	{
		snapxip = (TransactionId*)(snapshot + 1);
		snapsubxip = (TransactionId*)((char*)snapshot + subxipoff);
	}
	else
	{
		snapxip = snapshot->xip;
		snapsubxip = snapshot->subxip;
	}

	if (snapshot->xcnt > 0)
	{
		newsnap->xip = (TransactionId*)(newsnap + 1);
		memcpy(newsnap->xip, snapxip, snapshot->xcnt * sizeof(TransactionId));
	}
	else
		newsnap->xip = NULL;

	if (snapshot->subxcnt > 0 &&
		(!snapshot->suboverflowed || snapshot->takenDuringRecovery))
	{
		newsnap->subxip = (TransactionId*)((char*)newsnap + subxipoff);
		memcpy(newsnap->subxip,
			   snapsubxip,
			   snapshot->subxcnt * sizeof(TransactionId));
	}
	else
		newsnap->subxip = NULL;

	return dsa_newsnap;
}

static dsa_pointer
EbiSetRightBoundaryRecursive(dsa_pointer dsa_node, Snapshot snapshot)
{
	EbiNode tmp;
	dsa_pointer ret;

	ret = dsa_node;
	tmp = EbiConvertToNode(ebitree_dsa_area, ret);

	while (DsaPointerIsValid(tmp->right))
	{
		ret = tmp->right;
		tmp = EbiConvertToNode(ebitree_dsa_area, ret);
		EbiSetRightBoundary(tmp, snapshot, false);
	}

	return ret;
}

void
EbiDecreaseRefCount(dsa_pointer dsa_node)
{
	EbiNode node;
	uint32 refcnt;

	node = EbiConvertToNode(ebitree_dsa_area, dsa_node);
	refcnt = EbiDecreaseRefCountInternal(node);

	if (refcnt == 0)
	{
		EbiMpscEnqueue(ebitree_dsa_area, EbiTreeShmem->unlink_queue, dsa_node);
	}
}

static uint32
EbiDecreaseRefCountInternal(EbiNode node)
{
	uint32 ret;
	ret = pg_atomic_sub_fetch_u32(&node->refcnt, 1);
	return ret;
}

void
EbiUnlinkNodes(dsa_pointer dsa_ebitree,
			   dsa_pointer unlink_queue,
			   EbiSpscQueue delete_queue)
{
	dsa_pointer dsa_tmp;
	EbiTree ebitree;

	ebitree = EbiConvertToTree(ebitree_dsa_area, dsa_ebitree);

	dsa_tmp = EbiMpscDequeue(ebitree_dsa_area, unlink_queue);

	while (DsaPointerIsValid(dsa_tmp))
	{
		// Logical deletion
		EbiUnlinkNode(ebitree, dsa_tmp, delete_queue);

		dsa_tmp = EbiMpscDequeue(ebitree_dsa_area, unlink_queue);
	}
}

static void
EbiUnlinkNode(EbiTree ebitree, dsa_pointer dsa_node, EbiSpscQueue delete_queue)
{
	EbiNode node;

	node = EbiConvertToNode(ebitree_dsa_area, dsa_node);

	// Logical deletion, takes it off from the EBI-tree
	EbiUnlinkFromParent(node);

	// Prepare it for physical deletion
	EbiPushToGarbageQueue(delete_queue, node, dsa_node);

	// Compaction
	EbiCompactNode(ebitree, node->parent);
}

static void
EbiUnlinkFromParent(EbiNode node)
{
	EbiNode parent, curr;
	uint64 num_versions;
	dsa_pointer proxy_target;

	parent = EbiConvertToNode(ebitree_dsa_area, node->parent);

	Assert(parent != NULL);

	if (EbiIsLeftChild(node))
	{
		parent->left = InvalidDsaPointer;
	}
	else
	{
		parent->right = InvalidDsaPointer;
	}

	/* Version counter */
	curr = node;
	while (curr != NULL)
	{
		proxy_target = curr->proxy_target;

		num_versions = pg_atomic_read_u64(&curr->num_versions);
		num_versions = num_versions - (num_versions % EBI_VERSION_CHUNK_SIZE);
		pg_atomic_sub_fetch_u64(&EbiTreeShmem->num_versions, num_versions);

		curr = EbiConvertToNode(ebitree_dsa_area, proxy_target);
	}
}

static void
EbiPushToGarbageQueue(EbiSpscQueue delete_queue,
					  EbiNode node,
					  dsa_pointer dsa_node)
{
	EbiSpscEnqueue(delete_queue, node, dsa_node);
}

static void
EbiCompactNode(EbiTree ebitree, dsa_pointer dsa_node)
{
	EbiNode node, tmp;
	dsa_pointer proxy_target;
	uint32 original_height;

#ifdef PRINT_EBI_INFO
	uint32 current_left_most;
#endif

	proxy_target = dsa_node;
	node = EbiConvertToNode(ebitree_dsa_area, dsa_node);

	if (EbiHasParent(node) == false)
	{
		// When the root's child is being compacted
		EbiNode root;

		if (EbiHasLeftChild(node))
		{
			EbiLinkProxy(node->left, proxy_target);
			ebitree->root = node->left;
		}
		else
		{
			EbiLinkProxy(node->right, proxy_target);
			ebitree->root = node->right;
		}
		root = EbiConvertToNode(ebitree_dsa_area, ebitree->root);
		root->parent = InvalidDsaPointer;
	}
	else
	{
		EbiNode parent;
		dsa_pointer tmp_ptr;

		parent = EbiConvertToNode(ebitree_dsa_area, node->parent);

		// Compact the one-and-only child and its parent
		if (EbiIsLeftChild(node))
		{
			if (EbiHasLeftChild(node))
			{
				EbiLinkProxy(node->left, proxy_target);
				parent->left = node->left;
			}
			else
			{
				EbiLinkProxy(node->right, proxy_target);
				parent->left = node->right;
#ifdef PRINT_EBI_INFO
				EbiNode right_node = EbiConvertToNode(ebitree_dsa_area, 
						node->right);
				parent->left_most = right_node->left_most;
#endif
			}
			tmp = EbiConvertToNode(ebitree_dsa_area, parent->left);
		}
		else
		{
			if (EbiHasLeftChild(node))
			{
				EbiLinkProxy(node->left, proxy_target);
				parent->right = node->left;
			}
			else
			{
				EbiLinkProxy(node->right, proxy_target);
				parent->right = node->right;
			}
			tmp = EbiConvertToNode(ebitree_dsa_area, parent->right);
		}
		tmp->parent = node->parent;

		// Parent height propagation
#ifdef PRINT_EBI_INFO
		current_left_most = parent->left_most;

		tmp_ptr = node->parent;

		while (DsaPointerIsValid(tmp_ptr))
		{
			EbiNode curr, curr_parent;

			curr = EbiConvertToNode(ebitree_dsa_area, tmp_ptr);

			if (!DsaPointerIsValid(curr->parent)) {
				break;
			} else {
				curr_parent = EbiConvertToNode(ebitree_dsa_area, curr->parent);
				if (curr_parent->left_most == current_left_most ||
						!EbiIsLeftChild(curr))
					break;
			}

			curr_parent->left_most = current_left_most;

			tmp_ptr = curr->parent;
		}
#else
		tmp_ptr = node->parent;
		while (DsaPointerIsValid(tmp_ptr))
		{
			EbiNode curr, left, right;

			curr = EbiConvertToNode(ebitree_dsa_area, tmp_ptr);
			left = EbiConvertToNode(ebitree_dsa_area, curr->left);
			right = EbiConvertToNode(ebitree_dsa_area, curr->right);

			original_height = curr->height;

			curr->height = Max(left->height, right->height) + 1;

			if (curr->height == original_height)
			{
				break;
			}

			tmp_ptr = curr->parent;
		}
#endif
	}
}

static void
EbiLinkProxy(dsa_pointer dsa_proxy, dsa_pointer dsa_proxy_target)
{
	EbiNode proxy_node, proxy_target_node, tail;

	proxy_node = EbiConvertToNode(ebitree_dsa_area, dsa_proxy);
	tail = EbiConvertToNode(ebitree_dsa_area, proxy_node->proxy_list_tail);

	Assert(!DsaPointerIsValid(tail->proxy_target));

	/* Connect the proxy_target node's list with the proxy node's list */
	tail->proxy_target = dsa_proxy_target;

	/* Set the proxy node's tail to the appended list's end */
	proxy_target_node = EbiConvertToNode(ebitree_dsa_area, dsa_proxy_target);
	proxy_node->proxy_list_tail = proxy_target_node->proxy_list_tail;
}

void
EbiDeleteNodes(EbiSpscQueue delete_queue)
{
	EbiNode front;
	dsa_pointer dsa_ptr;

	while (!EbiSpscQueueIsEmpty(delete_queue))
	{
		dsa_ptr = EbiSpscQueueFrontDsaPointer(delete_queue);

		pg_memory_barrier();

		front = EbiSpscDequeue(delete_queue);

		EbiDeleteNode(front, dsa_ptr);
	}
}

void
EbiDeleteNode(EbiNode node, dsa_pointer dsa_ptr)
{
	EbiNode curr;
	dsa_pointer dsa_curr, proxy_target;

	curr = node;
	dsa_curr = dsa_ptr;

	while (curr != NULL)
	{
		proxy_target = curr->proxy_target;

		EbiDeleteSubNodes(curr);

		/* Leftboundary could be unset when EbiDeleteTree is called on shutdown
		*/
		if (DsaPointerIsValid(curr->left_boundary))
		{
			dsa_free(ebitree_dsa_area, curr->left_boundary);
		}

		/* Right boundary could be unset in the case of internal nodes */
		if (DsaPointerIsValid(curr->right_boundary))
		{
			dsa_free(ebitree_dsa_area, curr->right_boundary);
		}

		dsa_free(ebitree_dsa_area, dsa_curr);

		curr = EbiConvertToNode(ebitree_dsa_area, proxy_target);
		dsa_curr = proxy_target;
	}
}

EbiNode
EbiSift(TransactionId xmin, TransactionId xmax)
{
	uint32 my_slot;
	EbiNode ret;

	/* Must be place before entering the EBI-tree */
	my_slot = pg_atomic_read_u32(&EbiTreeShmem->curr_slot);
	pg_atomic_fetch_add_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);

	pg_memory_barrier();

	ret = EbiSiftInternal(xmin, xmax);

	EbiUpdateAverageVersionLifetime(xmin, xmax);

	pg_memory_barrier();

	/* Must be place after traversing the EBI-tree */
	pg_atomic_fetch_sub_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);

	return ret;
}

static EbiNode
EbiSiftInternal(TransactionId xmin, TransactionId xmax)
{
	EbiTree ebitree;
	EbiNode curr, left, right;
	bool left_includes, right_includes;
	bool left_exists, right_exists;

	ebitree = EbiConvertToTree(ebitree_dsa_area, EbiTreeShmem->ebitree);
	curr = EbiConvertToNode(ebitree_dsa_area, ebitree->root);

	Assert(curr != NULL);

	/* If root's left boundary doesn't set yet, return immediately */
	if (!DsaPointerIsValid(curr->left_boundary)) return NULL;

	/* The version is already dead, may be cleaned */
	if (!EbiOverlaps(curr, xmin, xmax)) return NULL;

	while ((curr != NULL) && !EbiIsLeaf(curr))
	{
		left = EbiConvertToNode(ebitree_dsa_area, curr->left);
		right = EbiConvertToNode(ebitree_dsa_area, curr->right);

		left_exists =
			((left != NULL) && DsaPointerIsValid(left->left_boundary));
		right_exists =
			((right != NULL) && DsaPointerIsValid(right->left_boundary));

		if (!left_exists && !right_exists)
		{
			return NULL;
		}
		else if (!left_exists)
		{
			// Only the left is null and the version does not fit into the right
			if (!EbiOverlaps(right, xmin, xmax))
			{
				return NULL;
			}
			else
			{
				curr = EbiConvertToNode(ebitree_dsa_area, curr->right);
			}
		}
		else if (!right_exists)
		{
			// Only the right is null and the version does not fit into the left
			if (!EbiOverlaps(left, xmin, xmax))
			{
				return NULL;
			}
			else
			{
				curr = EbiConvertToNode(ebitree_dsa_area, curr->left);
			}
		}
		else
		{
			// Both are not null
			left_includes = EbiOverlaps(left, xmin, xmax);
			right_includes = EbiOverlaps(right, xmin, xmax);

			if (left_includes && right_includes)
			{
				// Overlaps both child, current interval is where it fits
				break;
			}
			else if (left_includes)
			{
				curr = EbiConvertToNode(ebitree_dsa_area, curr->left);
			}
			else if (right_includes)
			{
				curr = EbiConvertToNode(ebitree_dsa_area, curr->right);
			}
			else
			{
				return NULL;
			}
		}
	}

	return curr;
}

static bool
EbiOverlaps(EbiNode node, TransactionId xmin, TransactionId xmax)
{
	Snapshot left_snap, right_snap;

	left_snap =
		(Snapshot)dsa_get_address(ebitree_dsa_area, node->left_boundary);
	right_snap =
		(Snapshot)dsa_get_address(ebitree_dsa_area, node->right_boundary);

	Assert(left_snap != NULL);

	if (right_snap != NULL)
		return XidInMVCCSnapshotForEBI(xmax, left_snap) &&
			!XidInMVCCSnapshotForEBI(xmin, right_snap);
	else
		return XidInMVCCSnapshotForEBI(xmax, left_snap);
}

EbiTreeVersionOffset
EbiSiftAndBind(TransactionId xmin,
			   TransactionId xmax,
			   Size tuple_size,
			   const void* tuple,
			   LWLock* rwlock)
{
	EbiNode node;
	EbiTreeSegmentId seg_id;
	EbiTreeSegmentOffset seg_offset;
	Size aligned_tuple_size;
	bool found;
	EbiTreeVersionOffset ret;
	uint64 num_versions;
	uint32 my_slot;

	Assert(ebitree_dsa_area != NULL);

	my_slot = pg_atomic_read_u32(&EbiTreeShmem->curr_slot);
	pg_atomic_fetch_add_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);
	pg_memory_barrier();

	/*
	 * After version sifting, we will get the correct ebi node to append the
	 * version.
	 */
	node = EbiSift(xmin, xmax);

	if (node == NULL)
	{
		pg_atomic_fetch_sub_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);
		/* Reclaimable */
		return EBI_TREE_INVALID_VERSION_OFFSET;
	}

	/* 
	 * Get ebi node's segment id. we use this segment id as pathfinder. 
	 * Segment id is unique and each ebi node has a different segment id.
	 */
	seg_id = node->seg_id;

	aligned_tuple_size = 1 << my_log2(tuple_size);

	/* We currently forbid tuples with sizes that are larger than the page size */
	Assert(aligned_tuple_size <= EBI_TREE_SEG_PAGESZ);

	do
	{
		seg_offset =
			pg_atomic_fetch_add_u32(&node->seg_offset, aligned_tuple_size);

		/* Checking if the tuple could be written within a single page */
		found = seg_offset / EBI_TREE_SEG_PAGESZ ==
				(seg_offset + aligned_tuple_size - 1) / EBI_TREE_SEG_PAGESZ;
	} while (!found);

	// Write version to segment
	EbiTreeAppendVersion(seg_id, seg_offset, tuple_size, tuple, rwlock);

	num_versions = pg_atomic_add_fetch_u64(&node->num_versions, 1);

	// Update global counter if necessary
	if (num_versions % EBI_VERSION_CHUNK_SIZE == 0)
	{
		pg_atomic_fetch_add_u64(&EbiTreeShmem->num_versions,
								EBI_VERSION_CHUNK_SIZE);
	}

	ret = EBI_TREE_SEG_TO_VERSION_OFFSET(seg_id, seg_offset);

	pg_atomic_fetch_sub_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);
	return ret;
}

int
EbiLookupVersion(EbiTreeVersionOffset version_offset,
				 Oid rel_id,
				 bool fast_memory_lookup,
				 uint32* tuple_size,
				 void** ret_value)
{
	EbiTreeSegmentId seg_id;
	EbiTreeSegmentOffset seg_offset;
	int buf_id;

	seg_id = EBI_TREE_VERSION_OFFSET_TO_SEG_ID(version_offset);
	seg_offset = EBI_TREE_VERSION_OFFSET_TO_SEG_OFFSET(version_offset);

	Assert(seg_id >= 1);
	Assert(seg_offset >= 0);

	/*
	 * Read version to ret_value in subnode. we use relation id
	 * to find correct subnode.
	 */
	buf_id = EbiSubReadVersionRef(seg_id, rel_id, seg_offset, 
								  tuple_size, ret_value, fast_memory_lookup);

	return buf_id;
}

bool
EbiSegIsAlive(dsa_pointer dsa_ebitree, EbiTreeSegmentId seg_id)
{
	bool ret;

	ret = EbiSegIsAliveInternal(dsa_ebitree, seg_id);

	pg_memory_barrier();

	return ret;
}

static bool
EbiSegIsAliveInternal(dsa_pointer dsa_ebitree, EbiTreeSegmentId seg_id)
{
	EbiTree ebitree;
	dsa_pointer dsa_curr, dsa_proxy;
	EbiNode curr, proxy;
	bool proxy_already_read;
	int node_index = 0;
	int seg_ids[MAX_NODE_HEIGHT];

	ebitree = EbiConvertToTree(ebitree_dsa_area, dsa_ebitree);

	dsa_curr = ebitree->root;
	curr = EbiConvertToNode(ebitree_dsa_area, dsa_curr);
	
	while (!EbiIsLeaf(curr))
	{
		if (curr->seg_id == seg_id)
		{
			return true;
		}

		dsa_proxy = curr->proxy_target;
		while (DsaPointerIsValid(dsa_proxy))
		{
			proxy = EbiConvertToNode(ebitree_dsa_area, dsa_proxy);

			/* for preventing double-read */
			proxy_already_read = false;
			if (proxy->proxy_list_tail != dsa_proxy)
			{
				for (int i = 0; i < node_index; i++)
				{
					if (proxy->seg_id == seg_ids[i])
					{
						proxy_already_read = true;
						break;
					}
				}
			}

			if (proxy_already_read)
				break;
			
			if (proxy->seg_id == seg_id)
				return true;

			dsa_proxy = proxy->proxy_target;
		}
		seg_ids[node_index++] = curr->seg_id;

		if (seg_id < curr->seg_id)
		{
			dsa_curr = curr->left;
		}
		else
		{
			dsa_curr = curr->right;
		}

		/* It has been concurrently removed by the EBI-tree process */
		if (!DsaPointerIsValid(dsa_curr))
		{
			return false;
		}

		curr = EbiConvertToNode(ebitree_dsa_area, dsa_curr);
	}

	Assert(EbiIsLeaf(curr));
  
	if (curr->seg_id == seg_id)
	{
		return true;
	}

	dsa_proxy = curr->proxy_target;
	while (DsaPointerIsValid(dsa_proxy))
	{
		proxy = EbiConvertToNode(ebitree_dsa_area, dsa_proxy);

		/* for preventing double-read */
		proxy_already_read = false;
		if (proxy->proxy_list_tail != dsa_proxy)
		{
			for (int i = 0; i < node_index; i++)
			{
				if (proxy->seg_id == seg_ids[i])
				{
					proxy_already_read = true;
					break;
				}
			}
		}

		if (proxy_already_read)
			break;
		
		if (proxy->seg_id == seg_id)
			return true;
			
		dsa_proxy = proxy->proxy_target;
	}

	return false;
}

bool
EbiRecentNodeIsAlive(dsa_pointer dsa_ebitree)
{
	EbiTree ebitree;
	EbiNode recent_node;
	bool ret;

	ebitree = EbiConvertToTree(ebitree_dsa_area, dsa_ebitree);
	recent_node = EbiConvertToNode(ebitree_dsa_area, ebitree->recent_node);

	ret = DsaPointerIsValid(recent_node->left_boundary);

	return ret;
}

EbiTree
EbiConvertToTree(dsa_area* area, dsa_pointer ptr)
{
	return (EbiTree)dsa_get_address(area, ptr);
}

EbiNode
EbiConvertToNode(dsa_area* area, dsa_pointer ptr)
{
	return (EbiNode)dsa_get_address(area, ptr);
}

/**
 * Utility Functions
 */

static bool
EbiHasParent(EbiNode node)
{
	return DsaPointerIsValid(node->parent);
}

static bool
EbiIsLeftChild(EbiNode node)
{
	EbiNode parent = EbiConvertToNode(ebitree_dsa_area, node->parent);
	return node == EbiConvertToNode(ebitree_dsa_area, parent->left);
}

static bool
EbiHasLeftChild(EbiNode node)
{
	return DsaPointerIsValid(node->left);
}

bool
EbiIsLeaf(EbiNode node)
{
	return node->height == 0;
}

static dsa_pointer
EbiSibling(EbiNode node)
{
	if (EbiHasParent(node))
	{
		EbiNode parent = EbiConvertToNode(ebitree_dsa_area, node->parent);
		if (EbiIsLeftChild(node))
		{
			return parent->right;
		}
		else
		{
			return parent->left;
		}
	}
	else
	{
		return InvalidDsaPointer;
	}
}

static void
EbiUpdateAverageVersionLifetime(TransactionId xmin, TransactionId xmax)
{
	uint32 len;

	/* Statisitcs for new node generation */
	len = xmax - xmin;

	/*
	 * Average value can be touched by multiple processes so that
	 * we need to decide only one process to update the value at a time.
	 */
	if (pg_atomic_test_set_flag(&EbiTreeShmem->is_updating_stat))
	{
		if (unlikely(EbiTreeShmem->average_ver_len == 0))
			EbiTreeShmem->average_ver_len = len;
		else
			EbiTreeShmem->average_ver_len =
				EbiTreeShmem->average_ver_len * EBI_AVG_STAT_WEIGHT +
				len * (1.0 - EBI_AVG_STAT_WEIGHT);

		pg_atomic_clear_flag(&EbiTreeShmem->is_updating_stat);
	}
}

static void
EbiUpdateTimespec(void)
{
	EbiGetCurrentTime(&EbiTreeShmem->last_timespec);
}

void
EbiMarkTupleSize(Size tuple_size)
{
	//Size new_size;

	if (likely(EbiTreeShmem->version_usage_check_flag))
	{
		// new_size = ((EbiTreeShmem->sampled_tuple_size * 3) +
		// 			tuple_size) >> 2;
		
		// EbiTreeShmem->sampled_tuple_size = new_size;
		return;
	}

	// EbiTreeShmem->sampled_tuple_size = tuple_size;
	EbiTreeShmem->version_usage_check_flag = true;
}

void
EbiPrintTree(dsa_pointer dsa_ebitree)
{
	EbiTree ebitree;
	EbiNode root;

	ebitree = EbiConvertToTree(ebitree_dsa_area, dsa_ebitree);
	root = EbiConvertToNode(ebitree_dsa_area, ebitree->root);

	ereport(LOG, (errmsg("Print Tree (%d)", root->height)));
	EbiPrintTreeRecursive(root);
}

void
EbiPrintTreeRecursive(EbiNode node)
{
	if (node == NULL)
	{
		return;
	}
	EbiPrintTreeRecursive(EbiConvertToNode(ebitree_dsa_area, node->left));
	ereport(LOG,
			(errmsg("[EBI] seg_id: %d, offset: %d",
					node->seg_id,
					pg_atomic_read_u32(&node->seg_offset))));
	EbiPrintTreeRecursive(EbiConvertToNode(ebitree_dsa_area, node->right));
}

#ifdef DIVA_PRINT
void PrintAllTree(dsa_pointer dsa_curr, int level, FILE* fp) {
	EbiNode curr;
	Snapshot left;
	Snapshot right;
	uint64 lseq;
	uint64 rseq;
	TransactionId lmin;
	TransactionId lmax;
	TransactionId rmin;
	TransactionId rmax;
	char *isleaf;

	if (!DsaPointerIsValid(dsa_curr))
		return;

	curr = EbiConvertToNode(ebitree_dsa_area, dsa_curr);
	left = (Snapshot)dsa_get_address(ebitree_dsa_area, curr->left_boundary);
	right = (Snapshot)dsa_get_address(ebitree_dsa_area, curr->right_boundary);
	lseq = left != NULL ? left->seq : 0;
	rseq = right != NULL ? right->seq : 0;
	lmin = left != NULL ? left->xmin : 0;
	lmax = left != NULL ? left->xmax : 0;
	rmin = right != NULL ? right->xmin : 0;
	rmax = right != NULL ? right->xmax : 0;
	isleaf = EbiIsLeaf(curr) ? "true" : "false";

	PrintAllTree(curr->left, level + 1, fp);

	char tmp_buf[2048];
	memset(tmp_buf, 0x00, sizeof(tmp_buf));
	int index = 0;
	for (int i = 0; i < level; ++i) {
		fprintf(fp, "> ");
	}
	// seg_id, seq nums
	fprintf(fp, "[[%u]]  :  [seq][%lu - %lu], [left][%u - %u], [right][%u - %u], leaf: %s\n", curr->seg_id, lseq, rseq, lmin, lmax, rmin, rmax, isleaf);
	
	PrintAllTree(curr->right, level + 1, fp);
}
#endif /* DIVA_PRINT */

#ifdef DIVA_PRINT
void PrintTreeToFile(int time) {
	FILE* fp;
	char filename[128];
	EbiTree ebitree;
	uint32 my_slot;
	EbiNode node;
	sprintf(filename, "treeshape.%08d", time);
	fp = fopen(filename, "w");


	ebitree = EbiConvertToTree(ebitree_dsa_area, EbiTreeShmem->ebitree);

	my_slot = pg_atomic_read_u32(&EbiTreeShmem->curr_slot);
	pg_atomic_fetch_add_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);

	node = EbiConvertToNode(ebitree_dsa_area, ebitree->root);
	PrintAllTree(ebitree->root, 0, fp);
	pg_atomic_fetch_sub_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);

	fclose(fp);
}
#endif /* DIVA_PRINT*/

/* EbiSubNode */
EbiSubNode
EbiConvertToSubNode(dsa_area* area, dsa_pointer ptr)
{
	return (EbiSubNode)dsa_get_address(area, ptr);
}

void
EbiDeleteSubNodes(EbiNode node)
{
	EbiSubNode subnode;
	int i;
	uint8 num_subnodes;

	num_subnodes = node->num_subnodes;

	for (i = 0; i < num_subnodes; i++)
	{
		subnode = EbiGetSubNode(node, i);
		EbiSubRemoveSegmentFile(subnode->seg_id, subnode->rel_id);

#ifdef LOCATOR
		for (int j = 0;
			 j < pg_atomic_read_u32(&(subnode->old_partitions_counter)); ++j)
			EbiSubRemoveOldPartitionFile(subnode->rel_id,
										 subnode->old_partitions[j]);
#endif /* LOCATOR */
	}

	for (i = 0; i < EBI_MAX_SUBNODES; i++)
	{
		dsa_free(ebitree_dsa_area, node->subnodes[i]);
	}
}

EbiSubNode
EbiSiftSubNode(EbiNode node, Oid rel_id)
{
	int i;
	uint8 num_subnodes;

	Assert(node != NULL);

	num_subnodes = node->num_subnodes;

	/*
	 * Opportunistic, you must ensure that physical subnode insertion is done
	 * before incrementing the num_subnodes count.
	 */
	for (i = 0; i < num_subnodes; i++)
	{
		if (node->rel_ids[i] == rel_id)
		{
			return EbiGetSubNode(node, i);
		}
	}

	/* Not found */
	return NULL;
}

EbiSubNode
EbiGetSubNode(EbiNode node, uint8 idx)
{
	Assert(idx < EBI_MAX_SUBNODES);
	return EbiConvertToSubNode(ebitree_dsa_area, node->subnodes[idx]);
}

EbiSubNode
EbiCreateSubNode(EbiNode node, Oid rel_id)
{
	EbiSubNode subnode;
	int i;

	LWLockAcquire(&node->lock, LW_EXCLUSIVE);

	/* Check if any other concurrent worker has inserted the subnode */
	for (i = 0; i < node->num_subnodes; i++)
	{
		if (node->rel_ids[i] == rel_id)
		{
			LWLockRelease(&node->lock);
			return EbiGetSubNode(node, i);
		}
	}

	subnode = EbiCreateSubNodeInternal(node, rel_id);

	LWLockRelease(&node->lock);

	return subnode;
}

EbiSubNode
EbiCreateSubNodeInternal(EbiNode node, Oid rel_id)
{
	/* All locks required are held from the callers. */
	EbiSubNode subnode;

	subnode = EbiGetSubNode(node, node->num_subnodes);

	Assert(subnode->rel_id == 0);
	Assert(subnode->seg_id == 0);

	subnode->seg_id = node->seg_id;
	subnode->rel_id = rel_id;
	subnode->max_page_id = 0;
#ifdef LOCATOR
	pg_atomic_init_u32(&(subnode->old_partitions_counter), 0);
#endif /* LOCATOR */

	EbiSubCreateSegmentFile(subnode->seg_id, subnode->rel_id);

	node->rel_ids[node->num_subnodes] = rel_id;
	pg_memory_barrier();
	++node->num_subnodes;

	return subnode;
}

bool
EbiSubOverlaps(EbiNode node, Snapshot snapshot)
{
	Snapshot left_snap, right_snap;

	left_snap =
		(Snapshot)dsa_get_address(ebitree_dsa_area, node->left_boundary);
	right_snap =
		(Snapshot)dsa_get_address(ebitree_dsa_area, node->right_boundary);

	Assert(left_snap != NULL);

	if (right_snap != NULL)
		return left_snap->seq < snapshot->seq &&
			   snapshot->seq < right_snap->seq;
	else
		return left_snap->seq < snapshot->seq;
}

static dsa_pointer
EbiSubCreateNodeWithHeight(dsa_area* area, uint32 height)
{
	dsa_pointer pointer;
	EbiNode node;

	dsa_pointer dsa_subnode;
	EbiSubNode subnode;
	int i;

	/* Allocate memory in dsa */
	pointer = dsa_allocate_extended(area, sizeof(EbiNodeData), DSA_ALLOC_ZERO);
	node = EbiConvertToNode(area, pointer);

	Assert(node != NULL);

	/* Initial values */
	node->parent = InvalidDsaPointer;
	node->left = InvalidDsaPointer;
	node->right = InvalidDsaPointer;
	node->proxy_target = InvalidDsaPointer;
	node->proxy_list_tail = pointer;
	node->height = height;
	pg_atomic_init_u32(&node->refcnt, 0);
	node->left_boundary = InvalidDsaPointer;
	node->right_boundary = InvalidDsaPointer;

	/* 
	 * Initialize file segment. The dedicated thread, alone, creates nodes.
	 */
	node->seg_id = ++EbiTreeShmem->seg_id;
#ifdef PRINT_EBI_INFO
	node->left_most = node->seg_id;
#endif

	Assert(node->seg_id != EBI_TREE_INVALID_SEG_ID);

	/* Create segment of ebi node */
	/* TODO: remove legacy code, now commented */
	//EbiTreeCreateSegmentFile(node->seg_id);
	//pg_atomic_init_u32(&node->seg_offset, 0);

	/* Version counter */
	pg_atomic_init_u64(&node->num_versions, 0);

	/* Initialize subnodes */
	for (i = 0; i < EBI_MAX_SUBNODES; i++)
	{
		dsa_subnode = dsa_allocate_extended(area, sizeof(EbiSubNodeData),
											DSA_ALLOC_ZERO);
		subnode = EbiConvertToSubNode(area, dsa_subnode);
		pg_atomic_init_u32(&subnode->seg_offset, 0);
		pg_atomic_init_u64(&subnode->num_versions, 0);
		
		node->subnodes[i] = dsa_subnode;
	}

	/* Initialize the number of subnode (i.e. zero). */
	node->num_subnodes = 0;

	pg_memory_barrier();

	/* Initialize subnode control lock */
	LWLockInitialize(&node->lock, LWTRANCHE_EBI_TREE);

	return pointer;
}


/* 
 * EbiSiftAndBindWithSub
 *
 * This method is used for ebi sifting and ebi subnode's binding.
 * In here, we get correct ebi node and append the version to the 
 * ebi's subnode. Because there are plenty of subnode in one ebi node,
 * we see relation oid to find the correct subnode.
 */
EbiTreeVersionOffset
EbiSiftAndBindWithSub(Oid rel_id, 
					  TransactionId xmin,
					  TransactionId xmax,
					  Size tuple_size,
					  const void* tuple,
					  LWLock* rwlock)
{
	EbiNode node;
	EbiTreeSegmentId seg_id;
	EbiTreeSegmentOffset seg_offset;
	Size aligned_tuple_size;
	bool found;
	EbiTreeVersionOffset ret;
	uint64 num_versions;
	uint32 my_slot;
	EbiSubNode subnode;

	Assert(ebitree_dsa_area != NULL);

	my_slot = pg_atomic_read_u32(&EbiTreeShmem->curr_slot);
	pg_atomic_fetch_add_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);
	pg_memory_barrier();

	/* After version sifting, we will get the correct ebi node to append the version. */
	node = EbiSift(xmin, xmax);

	if (node == NULL)
	{
		pg_atomic_fetch_sub_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);
		/* Reclaimable */
		return EBI_TREE_INVALID_VERSION_OFFSET;
	}

	/* 
	 * Get ebi node's segment id. we use this segment id as pathfinder. 
	 * Segment id is unique and each ebi node has a different segment id.
	 */
	seg_id = node->seg_id;

	/* EbiSubNode */
	subnode = EbiSiftSubNode(node, rel_id);
	if (subnode == NULL)
	{
		subnode = EbiCreateSubNode(node, rel_id);
	}
	Assert(subnode != NULL);

	/* Add metadata bytes for representing version length */
	aligned_tuple_size = 1 << my_log2(tuple_size + EBI_SUB_VERSION_METADATA);

	do
	{
		seg_offset =
		pg_atomic_fetch_add_u32(&subnode->seg_offset, aligned_tuple_size);

		/* Checking if the tuple could be written within a single page */
		found = seg_offset / EBI_TREE_SEG_PAGESZ ==
		(seg_offset + aligned_tuple_size - 1) / EBI_TREE_SEG_PAGESZ;
	} while (!found);

	/* 
	 * Write version to EbiSubNode. We should get a block to read a page.
	 * In here, we just use simple lru policy to evict pages.
	 */
	EbiSubAppendVersion(seg_id, rel_id, seg_offset, tuple_size, tuple, rwlock);

	pg_memory_barrier();

	/*
	 * TODO: race condition here
	 */
	if ((seg_offset % EBI_TREE_SEG_PAGESZ) == 0)
		subnode->max_page_id += 1;

	/*
	 * TODO: make version number statistic of subnode
	 */
	pg_atomic_add_fetch_u64(&subnode->num_versions, 1);

	num_versions = pg_atomic_add_fetch_u64(&node->num_versions, 1);

	// Update global counter if necessary
	if (num_versions % EBI_VERSION_CHUNK_SIZE == 0)
	{
		pg_atomic_fetch_add_u64(&EbiTreeShmem->num_versions,
								EBI_VERSION_CHUNK_SIZE);
	}

	ret = EBI_TREE_SEG_TO_VERSION_OFFSET(seg_id, seg_offset);

	pg_atomic_fetch_sub_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);

	return ret;
}

#if 0
/*
 * Get max page id of ebi-subnode.
 * After getting the page id, use these ids when scanning the page.
 */
static int
EbiGetMaxPageId(HeapScanDesc scan, EbiNode node, Oid rel_id)
{
	EbiSubNode subnode;

	/* EbiSubNode */
	subnode = EbiSiftSubNode(node, rel_id);
	if (subnode == NULL)
	{
		/*
		 * The logic of subnode creation is opportunistic, so even if this
		 * transaction judges that there is no subnode of this relation, some
		 * other writer transaction may be creating new subnode.
		 * 
		 * However, if some other writer transaction is creating new subnode,
		 * it means that the transaction is trying to append the old version
		 * from the heap page to ebi subnode, and since this transaction has
		 * already scanned the heap pages, there is no need to read that
		 * old version again.
		 * 
		 * Therefore, even if a new subnode is being created at this time, this
		 * transaction does not need to scan it.
		 */
		return 0;
	}

	if (subnode->max_page_id == 0)
		return 0;

	/* set end points */
	scan->ebi_pages[scan->max_seg_index].seg_id = subnode->seg_id;
	scan->ebi_pages[scan->max_seg_index].max_page_id = subnode->max_page_id;
	++(scan->max_seg_index);
	Assert(scan->max_seg_index < MAX_SEGMENTS_PER_EBI_SCAN);

	return subnode->max_page_id;
}

/*
 * Get all max page ids with snapshot by traversing ebi-tree.
 * Traverse root to leaf, including proxy nodes.
 */
void EbiGetPageIds(HeapScanDesc scan)
{
	Snapshot snapshot;
	Relation relation;

	uint32 my_slot;

	EbiTree ebitree;
	EbiNode curr, proxy, left, right;
	dsa_pointer dsa_proxy;
	bool proxy_already_read;
	int node_index = 0;
	int seg_ids[MAX_NODE_HEIGHT];
	
	bool left_exists, right_exists;
	
	Oid rel_id;
	bool is_leaf;
	int pages_read;
	
	snapshot = scan->rs_base.rs_snapshot;
	relation = scan->rs_base.rs_rd;
	rel_id = relation->rd_node.relNode;

	/* Must be placed before entering the EBI-tree */
	my_slot = pg_atomic_read_u32(&EbiTreeShmem->curr_slot);
	pg_atomic_fetch_add_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);

	pg_memory_barrier();

	ebitree = EbiConvertToTree(ebitree_dsa_area, EbiTreeShmem->ebitree);
	curr = EbiConvertToNode(ebitree_dsa_area, ebitree->root);

	/* If root's left boundary doesn't set yet, return immediately */
	if (!DsaPointerIsValid(curr->left_boundary)) return;

	Assert(curr != NULL);

	pages_read = 0;

	while ((curr != NULL))
	{
		is_leaf = EbiIsLeaf(curr);

		if (!EbiSubOverlaps(curr, snapshot))
		{
			break;
		}

		pages_read += EbiGetMaxPageId(scan, curr, rel_id);

		/*
		 * Traverse proxy nodes before child nodes.
		 */
		dsa_proxy = curr->proxy_target;
		while (DsaPointerIsValid(dsa_proxy))
		{
			proxy = EbiConvertToNode(ebitree_dsa_area, dsa_proxy);

			/* for preventing double-read */
			proxy_already_read = false;
			if (proxy->proxy_list_tail != dsa_proxy)
			{
				for (int i = 0; i < node_index; i++)
				{
					if (proxy->seg_id == seg_ids[i])
					{
						proxy_already_read = true;
						break;
					}
				}
			}

			if (proxy_already_read)
				break;
			
			pages_read += EbiGetMaxPageId(scan, proxy, rel_id);
			dsa_proxy = proxy->proxy_target;
		}

		seg_ids[node_index++] = curr->seg_id;
		Assert(node_index < MAX_NODE_HEIGHT);

		left = EbiConvertToNode(ebitree_dsa_area, curr->left);
		right = EbiConvertToNode(ebitree_dsa_area, curr->right);

		left_exists =
			((left != NULL) && DsaPointerIsValid(left->left_boundary));
		right_exists =
			((right != NULL) && DsaPointerIsValid(right->left_boundary));

		if (!left_exists && !right_exists)
		{
			break;
		}
		else if (!left_exists)
		{
			// Only the left is null and the version does not fit into the right
			if (EbiSubOverlaps(right, snapshot))
			{
				curr = right;
			}
			else
			{
				break;
			}
		}
		else if (!right_exists)
		{
			// Only the right is null and the version does not fit into the left
			if (EbiSubOverlaps(left, snapshot))
			{
				curr = left;
			}
			else
			{
				break;
			}
		}
		else
		{
			if (EbiSubOverlaps(left, snapshot))
			{
				curr = left;
			}
			else if (EbiSubOverlaps(right, snapshot))
			{
				curr = right;
			}
			else
			{
				break;
			}
		}

		if (is_leaf) break;
	}

	pg_memory_barrier();

	/* Must be placed after traversing the EBI-tree */
	pg_atomic_fetch_sub_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);

	elog(LOG, "@@@ pages_read(EbiGetPageIds): %d, max_seg_index: %d",
		 pages_read, scan->max_seg_index);
}
#endif

int
EbiTreeVersionCompare(const void *a, const void *b)
{
	EbiTreeVersionOffset first = *((EbiTreeVersionOffset*)a);
	EbiTreeVersionOffset second = *((EbiTreeVersionOffset*)b);
	
	if (first > second)
		return 1;
	
	if (first < second)
		return -1;

	/*
	 * Every unobtained version recorded is issued a unique offset using
	 * fetch_and_add when appended to the ebi page, so no two versions have the
	 * same offset.
	 */
	Assert(false);
	return 0;
}

bool IsEbiExist(Snapshot snapshot)
{
	uint32 my_slot;

	EbiTree ebitree;
	EbiNode curr, left, right;
	
	bool left_exists, right_exists;
	bool is_leaf;

	/* Must be placed before entering the EBI-tree */
	my_slot = pg_atomic_read_u32(&EbiTreeShmem->curr_slot);
	pg_atomic_fetch_add_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);

	pg_memory_barrier();

	ebitree = EbiConvertToTree(ebitree_dsa_area, EbiTreeShmem->ebitree);
	curr = EbiConvertToNode(ebitree_dsa_area, ebitree->root);

	/* If root's left boundary doesn't set yet, return immediately */
	if (!DsaPointerIsValid(curr->left_boundary)) return NULL;

	Assert(curr != NULL);

	while ((curr != NULL))
	{
		is_leaf = EbiIsLeaf(curr);

		if (!EbiSubOverlaps(curr, snapshot))
		{
			break;
		}

		left = EbiConvertToNode(ebitree_dsa_area, curr->left);
		right = EbiConvertToNode(ebitree_dsa_area, curr->right);

		left_exists =
			((left != NULL) && DsaPointerIsValid(left->left_boundary));
		right_exists =
			((right != NULL) && DsaPointerIsValid(right->left_boundary));

		if (!left_exists && !right_exists)
		{
			break;
		}
		else if (!left_exists)
		{
			// Only the left is null and the version does not fit into the right
			if (EbiSubOverlaps(right, snapshot))
			{
				curr = right;
			}
			else
			{
				break;
			}
		}
		else if (!right_exists)
		{
			// Only the right is null and the version does not fit into the left
			if (EbiSubOverlaps(left, snapshot))
			{
				curr = left;
			}
			else
			{
				break;
			}
		}
		else
		{
			if (EbiSubOverlaps(left, snapshot))
			{
				curr = left;
			}
			else if (EbiSubOverlaps(right, snapshot))
			{
				curr = right;
			}
			else
			{
				break;
			}
		}

		if (is_leaf) break;
	}

	pg_memory_barrier();

	/* Must be placed after traversing the EBI-tree */
	pg_atomic_fetch_sub_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);

	return is_leaf;
}

#ifdef LOCATOR
/* 
 * EbiSiftAndBindPartitionOldFile
 *
 * This method is used for old partition file sifting and ebi subnode's binding.
 * In here, we get correct ebi node and link the old partition file to the 
 * ebi's subnode. Because there are plenty of subnode in one ebi node,
 * we see relation oid to find the correct subnode.
 */
void
EbiSiftAndBindPartitionOldFile(Oid db_id,
							   Oid rel_id,
							   TransactionId xmin,
							   TransactionId xmax,
							   LocatorPartLevel part_lvl,
							   LocatorPartNumber part_num,
							   LocatorPartGenNo min_part_gen,
							   LocatorPartGenNo max_part_gen)
{
	EbiNode node;
	uint32 my_slot;
	EbiSubNode subnode;
	int index;

	Assert(ebitree_dsa_area != NULL);

    if (unlikely(EbiTreeShmem->isShutdown))
    {
        char filename[32];

        for (LocatorPartGenNo part_gen = min_part_gen;
             part_gen <= max_part_gen; ++part_gen)
        {
            snprintf(filename, 32, "base/%u/%u.%u.%u.%u",
                     db_id, rel_id, part_lvl, part_num, part_gen);

			if (unlink(filename) != 0)
				elog(ERROR, "EbiSiftAndBindPartitionOldFile error");
#ifdef LOCATOR_DEBUG
		    fprintf(stderr, "[LOCATOR] GC old partition file, name: %s\n", filename);
#endif /* LOCATOR_DEBUG */
        }

        return;
    }

	my_slot = pg_atomic_read_u32(&EbiTreeShmem->curr_slot);
	pg_atomic_fetch_add_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);
	pg_memory_barrier();

	/* After version sifting, we will get the correct ebi node to append the version. */
	node = EbiSift(xmin, xmax);
	Assert(node != NULL);

	/* EbiSubNode */
	subnode = EbiSiftSubNode(node, rel_id);
	if (subnode == NULL)
	{
		subnode = EbiCreateSubNode(node, rel_id);
	}
	Assert(subnode != NULL);

	/* Get index */
	index = pg_atomic_fetch_add_u32(&(subnode->old_partitions_counter), 1);
	Assert(index < LOCATOR_MAX_OLD_FILES);

	/* Set old partitions */
	subnode->old_partitions[index].db_id = db_id;
	subnode->old_partitions[index].part_lvl = part_lvl;
	subnode->old_partitions[index].part_num = part_num;
	subnode->old_partitions[index].min_part_gen = min_part_gen;
	subnode->old_partitions[index].max_part_gen = max_part_gen;

#ifdef LOCATOR_DEBUG
	fprintf(stderr, "[LOCATOR] link old file for GC, rel: %u level: %u, part: %u, min_gen: %u, max_gen: %u\n",
					rel_id, part_lvl, part_num, min_part_gen, max_part_gen);
#endif /* LOCATOR_DEBUG */

	pg_atomic_fetch_sub_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);
}
#endif /* LOCATOR */


#endif /* DIVA */
