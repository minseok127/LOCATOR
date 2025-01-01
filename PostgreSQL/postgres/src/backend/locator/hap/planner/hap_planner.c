/*-------------------------------------------------------------------------
 *
 * hap_planner.c
 *	  Planner for HAP
 *
 *
 * IDENTIFICATION
 *	  src/backend/locator/hap/planner/hap_planner.c
 *-------------------------------------------------------------------------
 */
#ifdef LOCATOR
#include "postgres.h"

#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "executor/executor.h"
#include "executor/nodeSeqscan.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "optimizer/appendinfo.h"
#include "optimizer/clauses.h"
#include "optimizer/inherit.h"
#include "optimizer/optimizer.h"
#include "optimizer/orclauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/placeholder.h"
#include "optimizer/planmain.h"
#include "utils/snapmgr.h"

#include "locator/hap/hap_hook.h"
#include "locator/hap/hap_planner.h"

#include "miscadmin.h"
#include "locator/hap/hap_reduction.h"

bool enable_hap_planner = true;
bool enable_hap_selectivity = false;

/*
 * hap_planner
 *		HAP's own planner related to hidden attribute.
 *
 * Planning related to hidden attribute goes through several stages.
 *
 * (1) If there is no dimension table to create hidden attribute quals, we can
 * exit the hap_planner. In this case, we do not consider hidden attribute.
 *
 * (2) Convert the quals of the dimension table into the quals for the hidden
 * attribute and propagates them to the fact tables. If there is no
 * transformable qual or if the selectivity is low even if converted,
 * it is meaningless to propagate. Then we can exit hap_planner immediately.
 *
 * (3) Join reduction is performed based on the hidden attribute quals
 * propagated to the fact tables.
 *
 * (4) Partition pruning is performed on the remaining fact tables after
 * join reduction.
 */
static void
hap_planner(PlannerInfo *root)
{
	if (!hap_check_dimension_table_existence(root))
		return;	

	if (!hap_propagate_hidden_attribute(root))
		return;
}

/*
 * Condition to hook the query_planner().
 */
HAP_DEFINE_HOOK_COND3(RelOptInfo *, query_planner,
					  PlannerInfo *, root,
					  query_pathkeys_callback, qp_callback,
					  void *, qp_extra)
{
	Assert(root->parse != NULL);
	return (root->parse->commandType == CMD_SELECT) && enable_hap_planner;
}

/*
 * Hook on query_planner().
 */
HAP_DEFINE_HOOK_BODY3(RelOptInfo *, query_planner,
					  PlannerInfo *, root,
					  query_pathkeys_callback, qp_callback,
					  void *, qp_extra)
{
	Query	   *parse = root->parse;
	List	   *joinlist;
	RelOptInfo *final_rel;

	/*
	 * Init planner lists to empty.
	 *
	 * NOTE: append_rel_list was set up by subquery_planner, so do not touch
	 * here.
	 */
	root->join_rel_list = NIL;
	root->join_rel_hash = NULL;
	root->join_rel_level = NULL;
	root->join_cur_level = 0;
	root->canon_pathkeys = NIL;
	root->left_join_clauses = NIL;
	root->right_join_clauses = NIL;
	root->full_join_clauses = NIL;
	root->join_info_list = NIL;
	root->placeholder_list = NIL;
	root->fkey_list = NIL;
	root->initial_rels = NIL;

	/*
	 * Set up arrays for accessing base relations and AppendRelInfos.
	 */
	setup_simple_rel_arrays(root);

	/*
	 * In the trivial case where the jointree is a single RTE_RESULT relation,
	 * bypass all the rest of this function and just make a RelOptInfo and its
	 * one access path.  This is worth optimizing because it applies for
	 * common cases like "SELECT expression" and "INSERT ... VALUES()".
	 */
	Assert(parse->jointree->fromlist != NIL);
	if (list_length(parse->jointree->fromlist) == 1)
	{
		Node	   *jtnode = (Node *) linitial(parse->jointree->fromlist);

		if (IsA(jtnode, RangeTblRef))
		{
			int			varno = ((RangeTblRef *) jtnode)->rtindex;
			RangeTblEntry *rte = root->simple_rte_array[varno];

			Assert(rte != NULL);
			if (rte->rtekind == RTE_RESULT)
			{
				/* Make the RelOptInfo for it directly */
				final_rel = build_simple_rel(root, varno, NULL);

				/*
				 * If query allows parallelism in general, check whether the
				 * quals are parallel-restricted.  (We need not check
				 * final_rel->reltarget because it's empty at this point.
				 * Anything parallel-restricted in the query tlist will be
				 * dealt with later.)  This is normally pretty silly, because
				 * a Result-only plan would never be interesting to
				 * parallelize.  However, if force_parallel_mode is on, then
				 * we want to execute the Result in a parallel worker if
				 * possible, so we must do this.
				 */
				if (root->glob->parallelModeOK &&
					force_parallel_mode != FORCE_PARALLEL_OFF)
					final_rel->consider_parallel =
						is_parallel_safe(root, parse->jointree->quals);

				/*
				 * The only path for it is a trivial Result path.  We cheat a
				 * bit here by using a GroupResultPath, because that way we
				 * can just jam the quals into it without preprocessing them.
				 * (But, if you hold your head at the right angle, a FROM-less
				 * SELECT is a kind of degenerate-grouping case, so it's not
				 * that much of a cheat.)
				 */
				add_path(final_rel, (Path *)
						 create_group_result_path(root, final_rel,
												  final_rel->reltarget,
												  (List *) parse->jointree->quals));

				/* Select cheapest path (pretty easy in this case...) */
				set_cheapest(final_rel);

				/*
				 * We don't need to run generate_base_implied_equalities, but
				 * we do need to pretend that EC merging is complete.
				 */
				root->ec_merging_done = true;

				/*
				 * We still are required to call qp_callback, in case it's
				 * something like "SELECT 2+2 ORDER BY 1".
				 */
				(*qp_callback) (root, qp_extra);

				return final_rel;
			}
		}
	}

	/*
	 * Construct RelOptInfo nodes for all base relations used in the query.
	 * Appendrel member relations ("other rels") will be added later.
	 *
	 * Note: the reason we find the baserels by searching the jointree, rather
	 * than scanning the rangetable, is that the rangetable may contain RTEs
	 * for rels not actively part of the query, for example views.  We don't
	 * want to make RelOptInfos for them.
	 */
	add_base_rels_to_query(root, (Node *) parse->jointree);

	/*
	 * Examine the targetlist and join tree, adding entries to baserel
	 * targetlists for all referenced Vars, and generating PlaceHolderInfo
	 * entries for all referenced PlaceHolderVars.  Restrict and join clauses
	 * are added to appropriate lists belonging to the mentioned relations. We
	 * also build EquivalenceClasses for provably equivalent expressions. The
	 * SpecialJoinInfo list is also built to hold information about join order
	 * restrictions.  Finally, we form a target joinlist for make_one_rel() to
	 * work from.
	 */
	build_base_rel_tlists(root, root->processed_tlist);

	find_placeholders_in_jointree(root);

	find_lateral_references(root);

	joinlist = deconstruct_jointree(root);

	/*
	 * Reconsider any postponed outer-join quals now that we have built up
	 * equivalence classes.  (This could result in further additions or
	 * mergings of classes.)
	 */
	reconsider_outer_join_clauses(root);

	/*
	 * If we formed any equivalence classes, generate additional restriction
	 * clauses as appropriate.  (Implied join clauses are formed on-the-fly
	 * later.)
	 */
	generate_base_implied_equalities(root);

#if 0 
	/*
	 * We have completed merging equivalence sets, so it's now possible to
	 * generate pathkeys in canonical form; so compute query_pathkeys and
	 * other pathkeys fields in PlannerInfo.
	 */
	(*qp_callback) (root, qp_extra);
#endif /* !HAP_REDUCTION */

	/*
	 * Examine any "placeholder" expressions generated during subquery pullup.
	 * Make sure that the Vars they need are marked as needed at the relevant
	 * join level.  This must be done before join removal because it might
	 * cause Vars or placeholders to be needed above a join when they weren't
	 * so marked before.
	 */
	fix_placeholder_input_needed_levels(root);

	/*
	 * Remove any useless outer joins.  Ideally this would be done during
	 * jointree preprocessing, but the necessary information isn't available
	 * until we've built baserel data structures and classified qual clauses.
	 */
	joinlist = remove_useless_joins(root, joinlist);

	/*
	 * Also, reduce any semijoins with unique inner rels to plain inner joins.
	 * Likewise, this can't be done until now for lack of needed info.
	 */
	reduce_unique_semijoins(root);

	/*
	 * Now distribute "placeholders" to base rels as needed.  This has to be
	 * done after join removal because removal could change whether a
	 * placeholder is evaluable at a base rel.
	 */
	add_placeholders_to_base_rels(root);

	/*
	 * Construct the lateral reference sets now that we have finalized
	 * PlaceHolderVar eval levels.
	 */
	create_lateral_join_info(root);

	/*
	 * Match foreign keys to equivalence classes and join quals.  This must be
	 * done after finalizing equivalence classes, and it's useful to wait till
	 * after join removal so that we can skip processing foreign keys
	 * involving removed relations.
	 */
	match_foreign_keys_to_quals(root);

	/*
	 * Look for join OR clauses that we can extract single-relation
	 * restriction OR clauses from.
	 */
	extract_restriction_or_clauses(root);

	/*
	 * At this point, all possible sub-queries have been merged into the
	 * main query, and quals are also distributed for each table.
	 * Using these, we can now perform HAP's own logic for planning.
	 */
	hap_planner(root);

#if 1
	if (enable_hap_reduction)
		joinlist = HapReducePlan(root, joinlist);

	/*
	 * We have completed merging equivalence sets, so it's now possible to
	 * generate pathkeys in canonical form; so compute query_pathkeys and
	 * other pathkeys fields in PlannerInfo.
	 */
	(*qp_callback) (root, qp_extra);
#endif

	/*
	 * Now expand appendrels by adding "otherrels" for their children.  We
	 * delay this to the end so that we have as much information as possible
	 * available for each baserel, including all restriction clauses.  That
	 * let us prune away partitions that don't satisfy a restriction clause.
	 * Also note that some information such as lateral_relids is propagated
	 * from baserels to otherrels here, so we must have computed it already.
	 */
	add_other_rels_to_query(root);

	/*
	 * Distribute any UPDATE/DELETE/MERGE row identity variables to the target
	 * relations.  This can't be done till we've finished expansion of
	 * appendrels.
	 */
	distribute_row_identity_vars(root);

	/*
	 * Ready to do the primary planning.
	 */
	final_rel = make_one_rel(root, joinlist);

	/* Check that we got at least one usable path */
	if (!final_rel || !final_rel->cheapest_total_path ||
		final_rel->cheapest_total_path->param_info != NULL)
		elog(ERROR, "failed to construct the join relation");

	return final_rel;
}
#endif /* LOCATOR */
