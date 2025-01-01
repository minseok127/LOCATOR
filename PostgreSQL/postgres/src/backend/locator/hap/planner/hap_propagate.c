/*-------------------------------------------------------------------------
 *
 * hap_propagate.c
 *	  Filter propagation for HAP
 *
 *
 * IDENTIFICATION
 *	  src/backend/locator/hap/planner/hap_propagate.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/attnum.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "nodes/bitmapset.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "optimizer/optimizer.h"
#include "optimizer/restrictinfo.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"

#include "locator/hap/hap_am.h"
#include "locator/hap/hap_catalog.h"
#include "locator/hap/hap_encoding.h"
#include "locator/hap/hap_planner.h"


#ifdef LOCATOR
/*
 * HapPropagateCond - Condition to determine whether or not to propagate.
 *
 * - ancestor_key: Foreign key attributes of propagating relation.
 * - descendant_key: Foreign key attributes of target relation.
 * - valid_keys: Bitmap indicating valid attributes in the key arrays.
 * - nkeys: Size of the key arrays.
 */
typedef struct HapPropagateCond
{
	AttrNumber ancestor_key[INDEX_MAX_KEYS];
	AttrNumber descendant_key[INDEX_MAX_KEYS];
	Bitmapset *valid_keys;
	int nkeys;
} HapPropagateCond;

/*
 * hap_init_propagate_cond
 *		Initialize searching propagation paths.
 *
 * ForeignKeyCacheInfo depends on relcache. So we can't modify it. If we modify
 * it some other functions can get the incorrect data.
 *
 * So we copy the foreign key informations to the HapPropagateCond. This allows
 * us to rearrange the key array to fit our logic.
 */
static void
hap_init_propagatation_path_search(HapPropagateCond *cond,
								   HapPropagatePath *path,
								   ForeignKeyCacheInfo *cachedfk,
					 			   Index target_rel_idx)
{
	int nkeys = cachedfk->nkeys;

	memcpy(cond->descendant_key, cachedfk->conkey, nkeys * sizeof(AttrNumber));
	memcpy(cond->ancestor_key, cachedfk->confkey, nkeys * sizeof(AttrNumber));
	cond->valid_keys = bms_add_range(NULL, 0, cachedfk->nkeys - 1);
	cond->nkeys = nkeys;
	
	/*
	 * We will step up to the referenced relation through the recursive
	 * function and the new referenced relation's oid will be added.
	 *
	 * So add the referenced table's oid at the end of list. Otherwise the
	 * entire list must be modified whenever new relation is appended.
	 */
	path->rel_oid_list = lappend_oid(NIL, cachedfk->conrelid);
	path->rel_oid_list = lappend_oid(path->rel_oid_list, cachedfk->confrelid);
	path->target_rel_idx = target_rel_idx;
}

/*
 * hap_save_propagate_state_before_recurse
 *		Copy the propagation state to the temporal variables.
 *
 * Variables that can be changed through a recursive function are stored to
 * temporal variables.
 * 
 * This allows the original state to be restored after the recursive function
 * returns.
 */
static void
hap_save_propagate_state_before_recurse(HapPropagateCond *cond,
										HapPropagateCond *tmp_cond,
										HapPropagatePath *path,
										HapPropagatePath *tmp_path)
{
	memcpy(tmp_cond->ancestor_key, cond->ancestor_key,
		   cond->nkeys * sizeof(AttrNumber));

	tmp_cond->valid_keys = bms_copy(cond->valid_keys);

	tmp_path->rel_oid_list = list_copy(path->rel_oid_list);
}

/*
 * hap_append_propagate_path_to_rel
 *		Append copied path to the RelOptInfo.
 *
 * The given path can be modified in recursive function. So we have to copy it
 * and append it to the RelOptInfo's list.
 */
static void
hap_append_propagate_path_to_rel(RelOptInfo *rel, HapPropagatePath *path)
{
	HapPropagatePath *newpath = palloc(sizeof(HapPropagatePath));
	newpath->rel_oid_list = list_copy(path->rel_oid_list);
	newpath->target_rel_idx = path->target_rel_idx;
	rel->hap_propagate_paths = lappend(rel->hap_propagate_paths, newpath);
}

/*
 * hap_rebuild_propagate_cond
 *		Rebuild propagation condition for the new ancestor's foreign key.
 * 
 * Rearrange cond's ancestor keys to match the new foreign key. At this time,
 * prev_cond containing the correct state is used.
 *
 * For example, 
 *
 *    C
 *    |  (B.1 = C.2) AND (B.2 = C.0)
 *    B
 *    |  (A.0 = B.1) AND (A.1 = B.0) AND (A.2 = B.2)
 *    A
 *
 * In this case, cond->descendant_key is set to {0, 1, 2} which means the
 * attribute numbers used for the foreign key of the propagation target A.
 *
 * Also, if prev_cond is storing propagation conditions for A and B,
 * prev_cond->ancestor_key would be set to {1, 0, 2} which is B's foreign key
 * attribute numbers.
 *
 * Current keys are set as below:
 *
 *    cond->ancestor_key = ??? => will be overwritten to the C's attributes.
 *    cond->descendant_key = {0, 1, 2} => A's attribute numbers
 *    prev_cond->ancestor_key = {1, 0, 2} => B's attribute numbers
 *
 * Note that cond->ancestor_key may have different keys with B's foreign key.
 * This is because the recursive function can overwrite the cond->ancestor_key.
 * So we use prev_cond->ancestor_key which stores the state before the recursive
 * function.
 *
 * Then we can figure out the relationship between the new ancestor C and the
 * existing ancestor B using cached foreign key:
 *
 *    cachedfk->conkey = {1, 2} => B's attribute numbers
 *    cachedfk->confkey = {2, 0} => C's attribute numbers
 *    
 * Now we will map cachedfk->conkey to prev_cond->ancestor_key and reset
 * cond->ancestor_key to cachedfk->confkey values. This allows us to know how
 * the keys of target A should be matched with the keys of C, the new ancestor.
 *
 *   cond->ancestor_key = {2, ?, 0} => C's attribute numbers
 *   cond->descendant_key = {0, 1, 2} => A's attribute numbers
 * 
 * => (A.0 = C.2) AND (A.2 = C.0)
 */
static bool
hap_rebuild_propagate_cond(HapPropagateCond *cond,
						   HapPropagateCond *prev_cond,
						   ForeignKeyCacheInfo *cachedfk)
{
	/*
	 * If the number of keys in the new foreign key is greater than the previous
	 * valid foreign key, it cannot be a subset.
	 */
	if (cachedfk->nkeys > bms_num_members(prev_cond->valid_keys))
		return false;

	/*
	 * Since we're storing the new ancestor's key, the valid attribute index
	 * must also change.
	 */
	bms_free(cond->valid_keys);
	cond->valid_keys = NULL;

	for (int i = 0; i < cachedfk->nkeys; i++)
	{
		AttrNumber attrnum1 = cachedfk->conkey[i];
		bool found = false;
		int j = -1;

		while ((j = bms_next_member(prev_cond->valid_keys, j)) >= 0)
		{
			AttrNumber attrnum2 = prev_cond->ancestor_key[j];

			if (attrnum2 == attrnum1)
			{
				/* Replace to the new ancestor's key */
				cond->ancestor_key[j] = cachedfk->confkey[i];
				cond->valid_keys = bms_add_member(cond->valid_keys, j);
				found = true;
			}
		}

		/* Not a subset */
		if (!found)
			return false;
	}

	return true;
}

/*
 * hap_redirect_propagate_path
 *		Redirect the propagation path from the previous state.
 *
 * A recursive function modifies the propagation path. Therefore, when it
 * returns, we must perfom a search for the new foreign key after going back to
 * the previous path.
 */
static void
hap_redirect_propagate_path(HapPropagatePath *path,
							HapPropagatePath *prev_path,
							ForeignKeyCacheInfo *cachedfk)
{
	list_free(path->rel_oid_list);
	path->rel_oid_list = list_copy(prev_path->rel_oid_list);
	path->rel_oid_list = lappend_oid(path->rel_oid_list, cachedfk->confrelid);
}

/*
 * hap_match_eclasses_to_propagate_key_col
 *		Check if an eclass exists for the given attributes.
 *
 * Find an eclass in which both given Vars are used. Return true if this exists.
 */
static bool
hap_match_eclasses_to_propagate_key_col(PlannerInfo *root,
										int var1varno,
										int var2varno,
										AttrNumber var1attno,
										AttrNumber var2attno)
{
	RelOptInfo *rel1 = root->simple_rel_array[var1varno];
	RelOptInfo *rel2 = root->simple_rel_array[var2varno];
	Bitmapset *matching_ecs;
	int i = -1;

	/* Get equivalence classes which contain Vars for both relations */
	Assert(IS_SIMPLE_REL(rel1));
	Assert(IS_SIMPLE_REL(rel2));
	matching_ecs = bms_intersect(rel1->eclass_indexes,
								 rel2->eclass_indexes);

	while ((i = bms_next_member(matching_ecs, i)) >= 0)
	{
		EquivalenceClass *ec = list_nth(root->eq_classes, i);
		EquivalenceMember *item1_em = NULL;
		EquivalenceMember *item2_em = NULL;
		ListCell *lc;

		foreach(lc, ec->ec_members)
		{
			EquivalenceMember *em = lfirst(lc);
			Var *var;

			/* EquivalenceMember must be a Var, possibly with RelabelType */
			var = (Var *) em->em_expr;
			while (var && IsA(var, RelabelType))
				var = (Var *) ((RelabelType *) var)->arg;
			if (!(var && IsA(var, Var)))
				continue;

			/* Match? */
			if (var->varno == var1varno && var->varattno == var1attno)
				item1_em = em;
			else if (var->varno == var2varno && var->varattno == var2attno)
				item2_em = em;

			if (item1_em && item2_em)
				return true;
		}
	}

	return false;
}

/*
 * hap_match_propagate_keys_to_quals
 *		Find out eclass for the keys used for propagation.
 *
 * Return true if the eclass exists for the keys used for propagation.
 */
static bool
hap_match_propagate_keys_to_quals(PlannerInfo *root,
								  HapPropagateCond *cond,
								  int ancestor_idx,
								  int descendant_idx)
{
	int valid_nkeys = bms_num_members(cond->valid_keys);
	int i = -1, nmatched_cols = 0;

	/*
	 * The key arrays are sorted so that the descendant key and ancestor key of
	 * the same index are connected.
	 */
	while ((i = bms_next_member(cond->valid_keys, i)) >= 0)
	{
		AttrNumber descendant_col = cond->descendant_key[i];
		AttrNumber ancestor_col = cond->ancestor_key[i];

		if (hap_match_eclasses_to_propagate_key_col(root,
													descendant_idx,
													ancestor_idx,
													descendant_col,
													ancestor_col))
			nmatched_cols++;
	}

	if (nmatched_cols == valid_nkeys)
		return true;

	return false;
}

/*
 * hap_try_set_propagate_paths
 *		Try to set propagation paths based on propagate cond.
 *
 * The given HapPropagateCond indicates the condition for the propagation of
 * hidden attribute.
 *
 * Among the relations used in this query, find RelOptInfo that satisfies the
 * condition and create a path.
 *
 * Return true if propagable path exists.
 */
static bool
hap_try_set_propagate_paths(PlannerInfo *root,
							HapPropagateCond *cond, HapPropagatePath *path)
{
	Index target_rel_idx = path->target_rel_idx;
	Oid ancestor_relid;
	bool found = false;
	int rti;

	Assert(path->rel_oid_list != NULL);
	ancestor_relid = llast_oid(path->rel_oid_list);

	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *rel = root->simple_rel_array[rti];
		RangeTblEntry *rte = root->simple_rte_array[rti];

		/* Ignore relations that cannot propagate */
		if ((rel == NULL) || (rel->reloptkind != RELOPT_BASEREL))
			continue;

		/* Ignore relations that are not currently indicated by the cond */
		if ((rte->relid != ancestor_relid) || (rte->relid == target_rel_idx))
			continue;

		/* Ignore non-HAP relations */
		if (!HapCheckAmUsingRelId(rte->relid))
			continue;

		if (hap_match_propagate_keys_to_quals(root, cond, rti, target_rel_idx))
		{
			hap_append_propagate_path_to_rel(rel, path);
			found = true;
		}
	}

	return found;
}


/*
 * hap_recurse_fkey_to_find_implicit_paths
 *		Find implicit propagation paths.
 *
 * Hidden attributes can be propagated by implicit relationships as well as
 * direct foreign key relationships.
 *
 * Suppose A refers to B and B refers to C. If the foreign key between B and C
 * is a subset of the foreign key between A and B, then C can propagate even
 * though it is not directly linked to A.
 */
static bool
hap_recurse_fkey_to_find_implicit_paths(PlannerInfo *root,
										HapPropagateCond *cond,
										HapPropagatePath *path)
{
	HapPropagateCond prev_cond = {0};
	HapPropagatePath prev_path = {0};
	Oid prev_ancestor_relid;
	Relation relation;
	bool found = false;
	List *cachedfkeys;
	ListCell *lc;
	
	check_stack_depth();

	/*
	 * Remember previous propagate states before recursing. This allows the
	 * modified propagation states to be restored to its previous states when
	 * the recursive function finishes and returns.
	 */
	hap_save_propagate_state_before_recurse(cond, &prev_cond, path, &prev_path);

	/* We want to move to the upper table. Get the new foreign key */
	Assert(prev_path.rel_oid_list != NULL);
	prev_ancestor_relid = llast_oid(prev_path.rel_oid_list);
	relation = table_open(prev_ancestor_relid, AccessShareLock);

	cachedfkeys = RelationGetFKeyList(relation);
	foreach(lc, cachedfkeys)
	{
		ForeignKeyCacheInfo *cachedfk = lfirst(lc);

		/*
		 * Rebuild the propagate cond to match the new ancestor. If the rebuild
		 * succeeds, try setting the propagation path.
		 *
		 * Note that if the rebuild fails, it means that the new foreign key is
		 * no longer a subset of the old foreign key. In this case, the
		 * recursion must stop.
		 */
		if (hap_rebuild_propagate_cond(cond, &prev_cond, cachedfk))
		{
			hap_redirect_propagate_path(path, &prev_path, cachedfk);
			found |= hap_try_set_propagate_paths(root, cond, path);
			found |= hap_recurse_fkey_to_find_implicit_paths(root, cond, path);
		}
	}

	table_close(relation, AccessShareLock);

	return found;
}

/*
 * hap_find_propagation_paths_for_rel
 *		Find paths that connect to the given rel.
 *
 * Get the foreign keys that a given rel references (child rel), and build
 * propagate infos based on them.
 *
 * Note that ForeignKeyCacheInfo depends on relcache. This means that modifying
 * this data may cause some other functions to get incorrect data.
 *
 * So we copy it into our own data, HapPropagateInfo. This allows us to
 * rearrange the keys in the recursive logic.
 */
static bool
hap_find_propagation_paths_for_rel(PlannerInfo *root, RelOptInfo *rel)
{
	bool found = false;
	RangeTblEntry *rte;
	Relation relation;
	List *cachedfkeys;
	ListCell *lc;

	/* Open the relation to get foreign keys */
	rte = root->simple_rte_array[rel->relid];
	relation = table_open(rte->relid, AccessShareLock);

	cachedfkeys = RelationGetFKeyList(relation);
	foreach(lc, cachedfkeys)
	{
		ForeignKeyCacheInfo *cachedfk = lfirst(lc);
		HapPropagateCond cond = {0};
		HapPropagatePath path = {0};

		/* Try setting propagation paths based on the given key */
		hap_init_propagatation_path_search(&cond, &path, cachedfk, rel->relid);
		found |= hap_try_set_propagate_paths(root, &cond, &path);

		/*
		 * This foreign key may contains implicit propagation paths. Check it.
		 */
		found |= hap_recurse_fkey_to_find_implicit_paths(root, &cond, &path);
	}

	table_close(relation, AccessShareLock);

	return found;
}

/*
 * hap_find_propagation_paths
 *		Find out which paths the filter can propagate.
 *
 * Hidden attribute can be propagated using JOIN with foreign key.
 *
 * Find the JOINs that are linked by foreign key, and create paths to propagate
 * the hidden attribute filter.
 *
 * Return true if the path exists.
 */
static bool
hap_find_propagation_paths(PlannerInfo *root)
{
	bool found = false;
	int rti;

	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *rel = root->simple_rel_array[rti];
		RangeTblEntry *rte = root->simple_rte_array[rti];

		/* Ignore relations that cannot be propagated to */
		if ((rel == NULL) || (rel->reloptkind != RELOPT_BASEREL))
			continue;

		/* Ignore non-HAP relations */
		if (!HapCheckAmUsingRelId(rte->relid))
			continue;

		found |= hap_find_propagation_paths_for_rel(root, rel);
	}

	return found;
}

/*
 * hap_create_hidden_attribute_encoding_intexpr
 *		Create Const representing int.
 *
 * Create an expression for integer using Const.
 */
static Const *
hap_create_hidden_attribute_intexpr(int value)
{
	Const *intexpr = makeNode(Const);

	intexpr->consttype = INT4OID;
	intexpr->consttypmod = -1;
	intexpr->constcollid = 0;
	intexpr->constlen = 4;
	intexpr->constvalue = Int32GetDatum(value);
	intexpr->constisnull = false;
	intexpr->constbyval = true;
	intexpr->location = -1;

	return intexpr;
}

/* 
 * hap_create_hidden_attribute_varexpr
 *		Create Var representing hidden attribute.
 *
 * Create an expression for a hidden attribute that is of type variable length
 * byte array.
 */
static Var *
hap_create_hidden_attribute_varexpr(int varno, int attno)
{
	Var *var = makeNode(Var);

	var->varno = varno;
	var->varattno = attno;
	var->vartype = BYTEAOID;
	var->vartypmod = -1;
	var->varlevelsup = 0;
	var->varnosyn = 1;
	var->varattnosyn = 1;
	var->location = -1;

	return var;
}

/*
 * hap_create_hidden_attribute_funcexpr
 *		Create FuncExpr representing hap_extract_encoding_value().
 *
 * To extract an encoding value from a hidden attribute, create a FuncExpr
 * representing the hap_extract_encoding_value() function.
 *
 * This goes into the first argument of OpExpr or ScalarArrayOpExpr for the
 * hidden attribute.
 */
static FuncExpr *
hap_create_hidden_attribute_funcexpr(HapHiddenAttrDesc *desc)
{
	FuncExpr *funcexpr = makeNode(FuncExpr);
	Const *arg2, *arg3;
	Var *arg1;

	/* Make FuncExpr (hap_extract_encoding_value) */
	funcexpr->funcid = 4552; /* See pg_proc.dat */
	funcexpr->funcresulttype = INT4OID;
	funcexpr->funcretset = false;
	funcexpr->funcvariadic = false;
	funcexpr->funcformat = COERCE_EXPLICIT_CALL;
	funcexpr->funccollid = 0;
	funcexpr->inputcollid = 0;
	funcexpr->location = -1;
	funcexpr->args = NIL;

	/* Make Var (1st argument, _hap_hidden_attribute) */
	arg1 = hap_create_hidden_attribute_varexpr(0, 0); /* dummy */

	/* Make Const (2nd argument, startbit) */
	arg2 = hap_create_hidden_attribute_intexpr(desc->startbit);

	/* Make Const (3rd argument, bitsize) */
	arg3 = hap_create_hidden_attribute_intexpr(desc->bitsize);

	/* Add the arguments to the FuncExpr */
	funcexpr->args = lappend(funcexpr->args, arg1);
	funcexpr->args = lappend(funcexpr->args, arg2);
	funcexpr->args = lappend(funcexpr->args, arg3);

	return funcexpr;
}

/*
 * hap_create_hidden_attribute_opexpr
 *		Create ScalarArrayOpExpr to hidden attribute.
 *
 * Used if there is only one encoding value.
 */
static Expr *
hap_create_hidden_attribute_opexpr(Oid relid, AttrNumber attno,
								   int value, Selectivity selec)
{
	int16 descid = HapGetDimensionHiddenAttrDescid(relid, attno);
	HapHiddenAttrOpExpr *hapopexpr;
	HapHiddenAttrDesc *desc;
	FuncExpr *funcexpr;
	Const *intexpr;
	OpExpr *opexpr;

	/* We already checked encode table exists, so it can't be -1 */
	Assert(descid >= 0);
	desc = HapMakeHiddenAttrDesc(relid, descid);

	hapopexpr = palloc0(sizeof(HapHiddenAttrOpExpr));
	hapopexpr->desc = desc;
	hapopexpr->value = value; 
	hapopexpr->selec = selec;

	/* Set OpExpr */
	opexpr = (OpExpr *) hapopexpr;
	opexpr->xpr.type = T_OpExpr;
	opexpr->opno = Int4EqualOperator;
	opexpr->opfuncid = F_INT4EQ;
	opexpr->opresulttype = BOOLOID;
	opexpr->opretset = false;
	opexpr->opcollid = 0;
	opexpr->inputcollid = 0; 
	opexpr->location = -1;
	opexpr->args = NIL;

	/* Make arguments */
	funcexpr = hap_create_hidden_attribute_funcexpr(desc);
	intexpr = hap_create_hidden_attribute_intexpr(value);

	/* Add the arguments */
	opexpr->args = lappend(opexpr->args, funcexpr);
	opexpr->args = lappend(opexpr->args, intexpr);

	return (Expr *) hapopexpr;
}

/*
 * hap_create_hidden_attribute_arrayexpr
 *		Create ArrayExpr to encoding values.
 *
 * Used to make the second argument of ScalarArrayOpEpxr to the hidden
 * attribute.
 */
static ArrayExpr *
hap_create_hidden_attribute_arrayexpr(List *values)
{
	ArrayExpr *arrayexpr = makeNode(ArrayExpr);
	ListCell *lc;

	arrayexpr->array_typeid = INT4ARRAYOID;
	arrayexpr->array_collid = 0;
	arrayexpr->element_typeid = INT4OID;
	arrayexpr->multidims = false;
	arrayexpr->location = -1;
	arrayexpr->elements = NIL;

	foreach(lc, values)
	{
		int value = lfirst_int(lc);
		arrayexpr->elements = lappend(arrayexpr->elements,
									  hap_create_hidden_attribute_intexpr(value));
	}

	return arrayexpr;
}

/*
 * hap_create_hidden_attribute_scalararrayopexpr
 *		Create ScalarArrayOpExpr to hidden attribute.
 *
 * Existing postgres converts ArrayExpr, the second argument of
 * ScalarArrayOpExpr, to Const through eval_const_expressions. Do the same
 * thing here as well.
 */
static Expr *
hap_create_hidden_attribute_scalararrayopexpr(PlannerInfo *root,
											  Oid relid, AttrNumber attno,
											  List *values,
											  Selectivity selec)
{
	int16 descid = HapGetDimensionHiddenAttrDescid(relid, attno);
	ScalarArrayOpExpr *arrayopexpr, *tmp_arrayopexpr;
	HapHiddenAttrScalarArrayOpExpr *haparrayopexpr;
	HapHiddenAttrDesc *desc;
	FuncExpr *funcexpr;
	ArrayExpr *arrayexpr;

	/* We already checked encode table exists, so it can't be -1 */
	Assert(descid >= 0);
	desc = HapMakeHiddenAttrDesc(relid, descid);

	haparrayopexpr = palloc0(sizeof(HapHiddenAttrScalarArrayOpExpr));
	haparrayopexpr->desc = desc;
	haparrayopexpr->value_list = values; 
	haparrayopexpr->selec = selec;

	/* Set ScalarArrayOpExpr */
	arrayopexpr = (ScalarArrayOpExpr *) haparrayopexpr;
	arrayopexpr->xpr.type = T_ScalarArrayOpExpr;
	arrayopexpr->opno = Int4EqualOperator;
	arrayopexpr->opfuncid = F_INT4EQ;
	arrayopexpr->hashfuncid = 0;
	arrayopexpr->negfuncid = 0;
	arrayopexpr->useOr = true;
	arrayopexpr->inputcollid = 0;
	arrayopexpr->location = -1;
	arrayopexpr->args = NIL;

	/* Make arguments */
	funcexpr = hap_create_hidden_attribute_funcexpr(desc);
	arrayexpr = hap_create_hidden_attribute_arrayexpr(values);

	/* Add the arguments */
	arrayopexpr->args = lappend(arrayopexpr->args, funcexpr);
	arrayopexpr->args = lappend(arrayopexpr->args, arrayexpr);

	/* Preprocess array expr to const, then return it */
	tmp_arrayopexpr = (ScalarArrayOpExpr *) eval_const_expressions(
												root, (Node *)arrayopexpr);

	/*
	 * Deep copy the preprocessed expr to the original.
	 */
	arrayopexpr->args = copyObject(tmp_arrayopexpr->args);

	return (Expr *) arrayopexpr;
}

/*
 * hap_create_hidden_attribute_expr_internal
 *		Create operational expression for hidden attribute.
 *
 * Create OpExpr/ScalarArrayOpExpr for hidden attribute. Both have Expr as their
 * first variable. So we use Expr as a return type.
 */
static Expr *
hap_create_hidden_attribute_expr_internal(PlannerInfo *root,
										  Index varno, Expr *originexpr)
{
	RangeTblEntry *rte = root->simple_rte_array[varno];
	Oid relid = rte->relid, encode_table_id;
	Bitmapset *varattnos = NULL;
	AttrNumber attrno;
	Selectivity selec;
	List *values;

	/* Operations with multiple attribute are not considered yet */
	pull_varattnos((Node *)originexpr, varno, &varattnos);
	if (bms_num_members(varattnos) != 1)
		return NULL;

	/* Ignore system attributes */
	attrno = bms_first_member(varattnos) + FirstLowInvalidHeapAttributeNumber;
	if (attrno < 1)
		return NULL;

	/* Check whether this attribute is encoded or not */
	encode_table_id = HapGetEncodeTableId(relid, attrno);
	if (encode_table_id == InvalidOid)
		return NULL;

	/* Get encoding values */
	values = hap_get_hidden_attribute_encoding_values(encode_table_id,
													  originexpr);
	if (values == NIL)
		return NULL;

	/* Calculate selectivity */
	selec = (double) list_length(values) / HapGetEncodeTableCardinality(relid,
																		attrno);
	/*
	 * If there is only one encoding value,
	 * we use OpExpr which means that "encoding value = val"
	 */
	if (list_length(values) == 1)
		return hap_create_hidden_attribute_opexpr(relid, attrno,
												  linitial_int(values), selec);
	/*
	 * If there are many encoding values, we use ScalarArrayOpExpr which means
	 * that "encoding value IN (val1, val2, val3 ...)"
	 */
	return hap_create_hidden_attribute_scalararrayopexpr(root, relid,
														 attrno, values, selec);
}

/*
 * hap_create_hidden_attribute_boolexpr
 *		Create boolean hidden attribute expression.
 *
 * HapHiddenAttrOpExpr and HapHiddenAttrScalarArrayOpExpr both have Expr as
 * their first variable. Therefore, the args list of HapHiddenAttrBoolExpr and
 * the args list of BoolExpr can point to the same list.
 */
static Expr *
hap_create_hidden_attribute_boolexpr(List *args, bool is_or)
{
	HapHiddenAttrBoolExpr *hidden_attribute_boolexpr;
	BoolExpr *boolexpr;
	Selectivity s;
	ListCell *lc;

	if (args == NIL)
		return NULL;

	/* Simplify it */
	if (list_length(args) == 1)
		return linitial(args);

	hidden_attribute_boolexpr = palloc0(sizeof(HapHiddenAttrBoolExpr));
	hidden_attribute_boolexpr->args = args;

	/* Set BoolExpr */
	boolexpr = (BoolExpr *) hidden_attribute_boolexpr;
	boolexpr->xpr.type = T_BoolExpr;
	boolexpr->args = args;
	boolexpr->location = -1;

	if (is_or)
	{		
		s = 0.0;

		/* Calculate selectivity */
		foreach(lc, args)
		{
			Expr *expr = lfirst(lc);

			if (IsA(expr, OpExpr))
			{
				HapHiddenAttrOpExpr *hapopexpr
					= (HapHiddenAttrOpExpr *)expr;
				s = s + hapopexpr->selec - s * hapopexpr->selec;
			}
			else
			{
				HapHiddenAttrScalarArrayOpExpr *haparrayopexpr
					= (HapHiddenAttrScalarArrayOpExpr *) expr;
				s = s + haparrayopexpr->selec - s * haparrayopexpr->selec;
			}
		}

		boolexpr->boolop = OR_EXPR;
	}
	else
	{
		s = 1.0;

		/* Calculate selectivity */
		foreach(lc, args)
		{
			Expr *expr = lfirst(lc);

			if (IsA(expr, OpExpr))
			{
				HapHiddenAttrOpExpr *hapopexpr
					= (HapHiddenAttrOpExpr *) expr;
				s *= hapopexpr->selec;
			}
			else
			{
				HapHiddenAttrScalarArrayOpExpr *haparrayopexpr
					= (HapHiddenAttrScalarArrayOpExpr *) expr;
				s *= haparrayopexpr->selec;
			}
		}

		boolexpr->boolop = AND_EXPR;
	}

	hidden_attribute_boolexpr->selec = s;

	return (Expr *) hidden_attribute_boolexpr;
}

/*
 * hap_create_sub_hidden_attribute_expr
 *		Create boolean expression for hidden attribute.
 *
 * Create AND/OR expression for hidden attribute. Note that in OR, if any of the
 * arguments does not conatin a hidden attribute, full OR cannot be used.
 * Otherwise, false negatives can occur.
 */
static Expr *
hap_create_sub_hidden_attribute_expr(PlannerInfo *root,
									 Index varno, Expr *originexpr)
{
	ListCell *lc;

	if (is_orclause(originexpr))
	{
		List *orlist = NIL;

		foreach(lc, ((BoolExpr *) originexpr)->args)
		{
			Expr *newexpr = hap_create_sub_hidden_attribute_expr(root, varno,
																 lfirst(lc));

			/*
			 * It can be NULL when the expr has no encoded attribute.
			 * Then abandon the whole OR clause.
			 */
			if (newexpr == NULL)
				return NULL;

			orlist = lappend(orlist, newexpr);
		}

		return hap_create_hidden_attribute_boolexpr(orlist, true);
	}
	else if (is_andclause(originexpr))
	{
		List *andlist = NIL;

		foreach(lc, ((BoolExpr *) originexpr)->args)
		{
			Expr *newexpr = hap_create_sub_hidden_attribute_expr(root, varno,
																 lfirst(lc));

			/*
			 * It can be NULL when the expr has no encoded attribute.
			 * Then we can ignore it.
			 */
			if (newexpr)
				andlist = lappend(andlist, newexpr);
		}

		return hap_create_hidden_attribute_boolexpr(andlist, false);
	}
	else
		return hap_create_hidden_attribute_expr_internal(root, varno,
														 originexpr);
}

/*
 * hap_create_hidden_attribute_expr
 *		Build a hidden attribute expr nodes.
 *
 * If the given RestrictInfo has a encoded attribute expression, change it to
 * the hidden attribute related expression.
 *
 * Because all hidden attribute expressions have Expr as their first member
 * variable, this function's return type is Expr.
 */
static Expr *
hap_create_hidden_attribute_expr(PlannerInfo *root,
								 Index varno, Expr *originexpr)
{
	if (is_orclause(originexpr))
		return hap_create_sub_hidden_attribute_expr(root, varno, originexpr);

	Assert(!is_andclause(originexpr));

	return hap_create_hidden_attribute_expr_internal(root, varno, originexpr);
}

/*
 * hap_transform_hidden_attribute_funcexpr
 *		Transform FuncExpr fot the given relation.
 *
 * FunExpr must be changed to the new relation, such as varno, attno, etc.
 */
static void
hap_transform_hidden_attribute_funcexpr(FuncExpr *funcexpr,
										HapHiddenAttrDesc *newdesc,
										Index newvarno)
{
	Var *arg1;
	Const *arg2, *arg3;

	Assert(list_length(funcexpr->args) == 3);

	/* Change varno and attrno */
	arg1 = linitial(funcexpr->args);
	arg1->varno = newvarno;
	arg1->varattno = get_attnum(newdesc->relid, "_hap_hidden_attribute");

	/* Change startbit */
	arg2 = lsecond(funcexpr->args);
	arg2->constvalue = newdesc->startbit;

	/* Change bitsize */
	arg3 = lthird(funcexpr->args);
	arg3->constvalue = newdesc->bitsize;
}

/*
 * hap_transform_hidden_attribute_opexpr
 *		Transform OpExpr for the given path.
 *
 * OpExpr's first argument must be changed to new relation.
 */
static Expr *
hap_transform_hidden_attribute_opexpr(HapPropagatePath *path, Expr *expr)
{
	HapHiddenAttrOpExpr *opexpr, *newopexpr;
	HapHiddenAttrDesc *desc, *newdesc;
	FuncExpr *newfuncexpr;
	int16 newdescid;

	opexpr = (HapHiddenAttrOpExpr *) expr;
	desc = opexpr->desc;

	/* Get the new target hidden attribute descriptor */
	newdescid = HapGetDescendantHiddenAttrDescid(path->rel_oid_list,
												 desc->descid, true);
	newdesc = HapMakeHiddenAttrDesc(linitial_oid(path->rel_oid_list),
									newdescid);

	/* Create new expr */
	newopexpr = palloc0(sizeof(HapHiddenAttrOpExpr));
	newopexpr->expr = opexpr->expr;
	newopexpr->desc = newdesc;
	newopexpr->value = opexpr->value;
	newopexpr->selec = opexpr->selec;

	/* List must be deep copied to change values safely */
	newopexpr->expr.args = copyObject(opexpr->expr.args);

	/* Change FuncExpr */
	newfuncexpr = linitial(newopexpr->expr.args);
	hap_transform_hidden_attribute_funcexpr(newfuncexpr, newdesc,
											path->target_rel_idx);

	return (Expr *) newopexpr;
}

/*
 * hap_transform_hidden_attribute_scalararrayopexpr
 *		Transform ScalarArrayOpExpr for the given path.
 *
 * ScalarArrayOpExpr's first argument must be changed to new relation.
 */
static Expr *
hap_transform_hidden_attribute_scalararrayopexpr(HapPropagatePath *path,
												 Expr *expr)
{
	HapHiddenAttrScalarArrayOpExpr *scalararrayopexpr, *newscalararrayopexpr;
	HapHiddenAttrDesc *desc, *newdesc;
	FuncExpr *newfuncexpr;
	int16 newdescid;

	scalararrayopexpr = (HapHiddenAttrScalarArrayOpExpr *) expr;
	desc = scalararrayopexpr->desc;

	/* Get the target hidden attribute descriptor */
	newdescid = HapGetDescendantHiddenAttrDescid(path->rel_oid_list,
												 desc->descid, true);
	newdesc = HapMakeHiddenAttrDesc(linitial_oid(path->rel_oid_list),
									newdescid);

	/* Create new expr */
	newscalararrayopexpr = palloc0(sizeof(HapHiddenAttrScalarArrayOpExpr));
	newscalararrayopexpr->expr = scalararrayopexpr->expr;
	newscalararrayopexpr->desc = newdesc;
	newscalararrayopexpr->value_list = scalararrayopexpr->value_list;
	newscalararrayopexpr->selec = scalararrayopexpr->selec;

	/* List must be deep copied to change value safely*/
	newscalararrayopexpr->expr.args = copyObject(scalararrayopexpr->expr.args);

	/* Change FuncExpr */
	newfuncexpr = linitial(newscalararrayopexpr->expr.args);
	hap_transform_hidden_attribute_funcexpr(newfuncexpr, newdesc,
											path->target_rel_idx);

	return (Expr *) newscalararrayopexpr;
}

/*
 * hap_transform_hidden_attribute_expr_interal
 * 		Transform non-boolean expressions for hidden attribute.
 *
 * Transfrom OpExpr/ScalarArrayOpExpr for the hidden attribute. Both have Expr
 * as their first variable. So we use Expr as a return type.
 */
static Expr *
hap_transform_hidden_attribute_expr_internal(HapPropagatePath *path, Expr *expr)
{
	if (IsA(expr, OpExpr))
		return hap_transform_hidden_attribute_opexpr(path, expr);
	else
		return hap_transform_hidden_attribute_scalararrayopexpr(path, expr);
}

/*
 * hap_transform_sub_hidden_attribute_expr
 *		Transform boolean expression for hidden attribute.
 *
 * Transform the AND/OR expr for the hidden attribute to fit the given path.
 * Note that unlike the hap_create_hidden_attribute() function, transformed
 * expressins can't be NULL.
 */
static Expr *
hap_transform_sub_hidden_attribute_expr(HapPropagatePath *path, Expr *expr)
{
	ListCell *lc;

	if (is_orclause(expr))
	{
		List *orlist = NIL;

		foreach(lc, ((BoolExpr *) expr)->args)
		{
			Expr *newexpr = hap_transform_sub_hidden_attribute_expr(path,
																 	lfirst(lc));

			orlist = lappend(orlist, newexpr);
		}

		return hap_create_hidden_attribute_boolexpr(orlist, true);
	}
	else if (is_andclause(expr))
	{
		List *andlist = NIL;

		foreach(lc, ((BoolExpr *) expr)->args)
		{
			Expr *newexpr = hap_transform_sub_hidden_attribute_expr(path,
																 	lfirst(lc));

			andlist = lappend(andlist, newexpr);
		}

		return hap_create_hidden_attribute_boolexpr(andlist, false);
	}
	else
		return hap_transform_hidden_attribute_expr_internal(path, expr);
}

/*
 * hap_transform_hidden_attribute_expr
 *		Transform the hidden attribute expr according to the given path.
 *
 * Transform expr to match the target table of path. Note that the first
 * element of path is the target and the last element is the table from which
 * the propagation begins.
 */
static Expr *
hap_transform_hidden_attribute_expr(HapPropagatePath *path, Expr *expr)
{
	if (is_orclause(expr))
		return hap_transform_sub_hidden_attribute_expr(path, expr);

	Assert(!is_andclause(expr));

	return hap_transform_hidden_attribute_expr_internal(path, expr);
}

/*
 * hap_propagate_hidden_attribute_expr_recurse
 *		Copy the expression and modify it to match the propagated table.
 *
 * Each table has an independent hidden attribute composition. Therefore, even
 * encodings for the same attribute may need to be interpreted in different ways
 * for different tables.
 *
 * To do this, the given expr is transformed and stored according to the each
 * table's hidden attribute composition while following the path.
 */
static void
hap_propagate_hidden_attribute_expr_recurse(PlannerInfo *root, List *paths,
											Expr *expr)
{
	ListCell *lc;

	check_stack_depth();

	foreach(lc, paths)
	{
		HapPropagatePath *path = lfirst(lc);
		Index targetidx = path->target_rel_idx;
		RelOptInfo *targetrel = root->simple_rel_array[targetidx];
		Expr *newexpr = hap_transform_hidden_attribute_expr(path, expr);

		/* Add the new expr to the targetrel */
		targetrel->hap_propagated_exprs
						= lappend(targetrel->hap_propagated_exprs, newexpr);

		/* Propagate along the path in the targetrel */
		hap_propagate_hidden_attribute_expr_recurse(root,
													targetrel->hap_propagate_paths,
													newexpr);
	}
}

/*
 * hap_propagate_base_hidden_attribute_exprs
 *		Propagate "base" clauses as hidden attribute expressions.
 *
 * RelOptInfo's baserestrictinfo is a RestrictInfo List. It's elements are
 * composed of single table clauses.
 *
 * Create hidden attribute expressions using them and propagate through foreign
 * key path.
 */
static bool
hap_propagate_base_hidden_attribute_exprs(PlannerInfo *root, RelOptInfo *rel)
{
	List *paths = rel->hap_propagate_paths;
	Index varno = rel->relid;
	bool propagated = false;
	ListCell *lc;

	foreach(lc, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = lfirst(lc);
		Expr *newexpr;

		/* It will be propagated as a joininfo */
		if (rinfo->hap_info_do_not_propagate)
			continue;

		newexpr = hap_create_hidden_attribute_expr(root, varno, rinfo->clause);

		/* This RestrictInfo can make hidden atttribute expression */
		if (newexpr != NULL)
		{
			Selectivity selec;

			if (IsA(newexpr, OpExpr))
			{
				HapHiddenAttrOpExpr *hapopexpr
					= (HapHiddenAttrOpExpr *) newexpr;
				selec = hapopexpr->selec;
			}
			else if (IsA(newexpr, ScalarArrayOpExpr))
			{
				HapHiddenAttrScalarArrayOpExpr *haparrayopexpr
					= (HapHiddenAttrScalarArrayOpExpr *) newexpr;
				selec = haparrayopexpr->selec;
			}
			else
			{
				HapHiddenAttrBoolExpr *hapboolexpr
					= (HapHiddenAttrBoolExpr *) newexpr;
				selec = hapboolexpr->selec;
			}

			/* If this expression has too bad selectiviy, ignore it */
			if (selec >= 0.7)
				continue;

			hap_propagate_hidden_attribute_expr_recurse(root, paths, newexpr);
			rinfo->hap_info_has_propagated = true;
			propagated = true;
		}
	}

	return propagated;
}

/*
 * Find HapJoinInfoExpr corresponding to the given original_joininfo_expr.
 */
static HapJoinInfoExpr *
hap_find_propagated_joininfo_expr(RelOptInfo *rel, Expr *original_joininfo_expr)
{
	HapJoinInfoExpr *new_joininfo_expr;
	ListCell *lc;

	foreach(lc, rel->hap_propagated_joininfo_exprs)
	{
		HapJoinInfoExpr *joininfo_expr
			= (HapJoinInfoExpr *) lfirst(lc);

		if (joininfo_expr->original_joininfo_expr == original_joininfo_expr)
			return joininfo_expr;
	}

	new_joininfo_expr = palloc0(sizeof(HapJoinInfoExpr));
	new_joininfo_expr->original_joininfo_expr = original_joininfo_expr;

	/* Its top operation must be OR */
	new_joininfo_expr->expr.xpr.type = T_BoolExpr;
	new_joininfo_expr->expr.boolop = OR_EXPR;
	new_joininfo_expr->expr.args = NIL;
	new_joininfo_expr->expr.location = -1;

	rel->hap_propagated_joininfo_exprs
		= lappend(rel->hap_propagated_joininfo_exprs, new_joininfo_expr);

	return new_joininfo_expr;
}

/*
 * Return the ith argument of the given argument list.
 */
static Expr *
hap_get_arg_from_boolexpr(BoolExpr *boolexpr, int i)
{
	List *args = boolexpr->args;

	if (args == NIL)
		return NULL;

	if (list_length(args) - 1 < i)
		return NULL;

	return list_nth(args, i);
}

/*
 * Const is placeholder of hidden attribute joininfo exprs
 */
static inline bool
hap_is_placeholder_of_joininfo(Expr *expr)
{
	return expr == NULL || IsA(expr, Const);
}

/*
 * hap_put_expr_to_boolexpr
 *		Put the given new expression to the given boolexpr's argument list.
 *
 * Insert the provided new expression into the argument list of the given
 * boolean expression at a specific position. If the list is not long enough,
 * create placeholders until the specified index.
 *
 * One thing to note is that several hidden attribute expressions can be
 * propagated through different paths. In such cases, these expressions should
 * be bound together using an AND clause.
 */
static void
hap_put_expr_to_boolexpr_args(BoolExpr *boolexpr, int index, Expr *newexpr)
{
	/*
	 * Make placeholders before the given index.
	 * FIXME Note that there must be at least 2 arguments for boolexpr.
	 */
	while (boolexpr->args == NIL || list_length(boolexpr->args) < index
			|| list_length(boolexpr->args) < 2)
	{
		if (boolexpr->boolop == AND_EXPR)
			boolexpr->args = lappend(boolexpr->args,
									 makeBoolConst(true, false));
		else /* OR */
			boolexpr->args = lappend(boolexpr->args,
									 makeBoolConst(false, false));
	}

	/* If this newexpr is the first to enter the given index, just append it */
	if (list_length(boolexpr->args) == index)
		boolexpr->args = lappend(boolexpr->args, newexpr);
	/* If there is some expression already placed */
	else
	{
		Expr *tmp_expr = (Expr *) list_nth(boolexpr->args, index);

		/* Is this a placeholder? replace it to the new expression */
		if (hap_is_placeholder_of_joininfo(tmp_expr))
		{
			/* remove placeholder */
			Assert(tmp_expr != NULL);
			pfree(tmp_expr);

			/* replace */
			lfirst(list_nth_cell(boolexpr->args, index)) = newexpr;
		}
		/* Or is this expression BoolExpr? then append new to it */
		else if (IsA(tmp_expr, BoolExpr))
			((BoolExpr *) tmp_expr)->args
				= lappend(((BoolExpr *) tmp_expr)->args, newexpr);
		/*
		 * Or is there already an expression that came from another path?
		 * Bind it with new expression.
		 */
		else
		{
			BoolExpr *new_and = (BoolExpr *) makeBoolExpr(AND_EXPR, NIL, -1);

			new_and->args = lappend(new_and->args, tmp_expr);
			new_and->args = lappend(new_and->args, newexpr);

			lfirst(list_nth_cell(boolexpr->args, index)) = new_and;
		}
	}
}

/*
 * hap_place_hidden_attribute_expr_to_joininfo
 *		Place the given newexpr to the right place in joininfo clause.
 *
 * HapJoinInfoExprLocation contains a List of HapJoinInfoBoolIndicator that
 * indicates which argument position to navigate to when encounting BoolExpr in
 * an expression tree.
 *
 * Using this information, put the new expression node to the proper node.
 */
static void
hap_place_hidden_attribute_expr_to_joininfo(RelOptInfo *rel,
											HapJoinInfoExprLocation *location,
											Expr *newexpr)
{
	HapJoinInfoBoolIndicator *bool_indicator
		= (HapJoinInfoBoolIndicator *) linitial(location->bool_indicator_list);
	HapJoinInfoExpr *joininfo_expr
		= hap_find_propagated_joininfo_expr(rel,
											location->original_joininfo_expr);
	BoolExpr *boolexpr = &joininfo_expr->expr, *prev_boolexpr = boolexpr;
	int index = bool_indicator->index;
	ListCell *lc;

	/* Start from second bool indicator */
	for_each_from(lc, location->bool_indicator_list, 1)
	{
		bool_indicator = (HapJoinInfoBoolIndicator *) lfirst(lc);

		/* Get the child boolean expression */
		boolexpr = (BoolExpr *) hap_get_arg_from_boolexpr(prev_boolexpr, index);

		/* Not yet initialized, initialize it */
		if (hap_is_placeholder_of_joininfo((Expr *) boolexpr))
		{
			boolexpr = (BoolExpr *) makeBoolExpr(bool_indicator->booltype,
												 NIL, -1);

			hap_put_expr_to_boolexpr_args(prev_boolexpr, index,
										  (Expr *) boolexpr);
		}

		Assert(IsA(boolexpr, BoolExpr));
		Assert(boolexpr->boolop == bool_indicator->booltype);

		prev_boolexpr = boolexpr;
		index = bool_indicator->index;
	}

	hap_put_expr_to_boolexpr_args(prev_boolexpr, index, newexpr);
}

/*
 * hap_propagate_joininfo_hidden_attribute_expr_recurse
 *		Copy the expression and modify it to match the propagated table.
 *
 * This is nearly idential to hap_propagate_hidden_attribute_expr_recurse(),
 * but it has subtle difference.
 * 
 * Unlike that function, we cannot use the transformed expression directly.
 * We have to place it to the right node of a joininfo expr.
 */
static void
hap_propagate_joininfo_hidden_attribute_expr_recurse(PlannerInfo *root,
			List *paths, HapJoinInfoExprLocation *location, Expr *expr)
{
	ListCell *lc;

	check_stack_depth();

	foreach(lc, paths)
	{
		HapPropagatePath *path = lfirst(lc);
		Index targetidx = path->target_rel_idx;
		RelOptInfo *targetrel = root->simple_rel_array[targetidx];
		Expr *newexpr = hap_transform_hidden_attribute_expr(path, expr);

		/* Apply the newexpr to the target rel's hidden attribute joininfo */
		hap_place_hidden_attribute_expr_to_joininfo(targetrel, location,
													newexpr);

		/* Propagate along the path in the targetrel */
		hap_propagate_joininfo_hidden_attribute_expr_recurse(root,
						targetrel->hap_propagate_paths, location, newexpr);
	}
}

/*
 * hap_propagate_joininfo_hidden_attribute_expr
 *		Create hidden attribute expr and propagate it to the joininfo expr.
 *
 * The given location represents a single clause which targets the given rel.
 *
 * Transform it to the hidden attribute expression and propagate it to a
 * hidden attribute joininfo expr of the child relations through foreign key
 * relationship path.
 */
static bool
hap_propagate_joininfo_hidden_attribute_expr(PlannerInfo *root, RelOptInfo *rel,
											 HapJoinInfoExprLocation *location)
{
	List *paths = rel->hap_propagate_paths;
	Expr *newexpr = NULL;

	if (paths == NIL)
		return false;

	newexpr = hap_create_hidden_attribute_expr(root, rel->relid,
											   location->target_expr);

	Assert(newexpr != NULL);

	hap_propagate_joininfo_hidden_attribute_expr_recurse(root, paths,
														 location, newexpr);

	return true;
}

/*
 * hap_propagate_joininfo_hidden_attribute_exprs
 *		Propagate "joininfo" clauses as hidden attribute expressions.
 *
 * Joininfo means a filter clause which contains multiple tables.
 *
 * We analyzed it at the hap_consider_propagating_joininfo() and stored the
 * informations to the RelOptInfo->hap_propagating_joininfo_maps.
 *
 * Using this information, make hidden attribute expressions and propagate
 * through foreign key path.
 */
static bool
hap_propagate_joininfo_hidden_attribute_exprs(PlannerInfo *root,
											  RelOptInfo *rel)
{
	bool propagated = false;
	ListCell *lc;

	/* Each map represents a single joininfo clause */
	foreach(lc, rel->hap_propagating_joininfo_maps)
	{
		HapJoinInfoMap *map = (HapJoinInfoMap *) lfirst(lc);
		ListCell *lc2;

		/*
		 * Each location represents a single "base" clause in the given
		 * joininfo and "base" means single-table clause.
		 *
		 * In other words, each location represents those that apply to the
		 * given RelOptInfo among the clauses contained in the joininfo.
		 */
		foreach(lc2, map->expr_location_list)
		{
			HapJoinInfoExprLocation *location
				= (HapJoinInfoExprLocation *) lfirst(lc2);

			propagated |= hap_propagate_joininfo_hidden_attribute_expr(root,
																	   rel,
																	   location);
		}
	}

	return propagated;
}

/*
 * __hap_propagate_hidden_attribute_exprs
 *		Workhorse for hap_propagate_hidden_attribute_exprs().
 *
 * Lookup encoded attributes and get encoding values from them.
 * Using the values, create hidden attribute quals and propagate them.
 */
static inline bool
__hap_propagate_hidden_attribute_exprs(PlannerInfo *root,
												  RelOptInfo *rel)
{
	return hap_propagate_base_hidden_attribute_exprs(root, rel)
		|| hap_propagate_joininfo_hidden_attribute_exprs(root, rel);
}

/*
 * hap_propagate_hidden_attribute_exprs
 *		The first step of hap_propagate_filter_predicates().
 *
 * Create hidden attribute exprs and propagate them to the fact tables.
 */
static bool
hap_propagate_hidden_attribute_exprs(PlannerInfo *root)
{
	bool propagated = false;

	for (int rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *rel = root->simple_rel_array[rti];
		RangeTblEntry *rte = root->simple_rte_array[rti];

		/* Ignore subquries */
		if ((rel == NULL) || (rel->reloptkind != RELOPT_BASEREL))
			continue;

		/* Ignore relations that has no paths */
		if (rel->hap_propagate_paths == NIL)
			continue;

		/* Ignore non-encoded relations */
		if (!HapRelidIsEncoded(rte->relid))
			continue;

		/* Do the real thing */
		if (__hap_propagate_hidden_attribute_exprs(root, rel))
			propagated = true;
	}

	return propagated;
}

/*
 * Workhorse for hap_make_restrictinfos_from_hidden_attribute_exprs().
 */
static void
__hap_make_restrictinfos_from_hidden_attribute_exprs(PlannerInfo *root,
													 RelOptInfo *rel,
													 Expr *clause)
{
	RestrictInfo *rinfo = make_restrictinfo(root,
											clause,
											true,
											false,
											false,
											0,
											pull_varnos(root, (Node *)clause),
											NULL,
											NULL);

	Assert(rinfo != NULL);

	rel->baserestrictinfo = lappend(rel->baserestrictinfo, rinfo);

	/*
	 * Set the flag to notice that this info is about hidden attribute
	 * and set the selectivity which is calculated already.
	 */
	rinfo->hap_info_is_hidden_attribute = true;
	if (IsA(clause, OpExpr))
	{
		HapHiddenAttrOpExpr *hapopexpr
			= (HapHiddenAttrOpExpr *) clause;
		rinfo->hap_hidden_attribute_selectivity = hapopexpr->selec;
	}
	else if (IsA(clause, ScalarArrayOpExpr))
	{
		HapHiddenAttrScalarArrayOpExpr *haparrayopexpr
			= (HapHiddenAttrScalarArrayOpExpr *) clause;
				rinfo->hap_hidden_attribute_selectivity = haparrayopexpr->selec;
	}
	else
	{
		HapHiddenAttrBoolExpr *hapboolexpr
			= (HapHiddenAttrBoolExpr *) clause;
		rinfo->hap_hidden_attribute_selectivity = hapboolexpr->selec;
	}
}

/*
 * hap_make_restrictinfos_from_hidden_attribute_exprs
 *		Create RestrictInfos from hidden attribute Exprs.
 *
 * hap_create_and_propagate_hidden_attribute_exprs() stores the expr of the
 * hidden attribute i hap_propagated_exprs of each RelOptInfo.
 *
 * Convert these to RestrictInfo so that they can be used in ExecQual().
 */
static void
hap_make_restrictinfos_from_hidden_attribute_exprs(PlannerInfo *root)
{
	for (int rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *rel = root->simple_rel_array[rti];
		ListCell *lc;

		if (rel == NULL)
			continue;

		foreach(lc, rel->hap_propagated_exprs)
			__hap_make_restrictinfos_from_hidden_attribute_exprs(root, rel,
																 lfirst(lc));

		foreach(lc, rel->hap_propagated_joininfo_exprs)
			__hap_make_restrictinfos_from_hidden_attribute_exprs(root, rel,
																 lfirst(lc));
	}
}

/*
 * hap_propagate_filter_predicates
 *		Get the encoding values and transform it to the hidden attribute exprs.
 *
 * Propagating hidden attribute quals consists of three steps.
 *
 * (1) Creating and propagating hidden attribute exprs.
 *     => Encoding values are specified from existing exprs, and hidden
 *     attribute exprs are created based on them. After that, they are
 *     propagated based on the previously identified path.
 *
 * (3) Transforming hidden attribute exprs to RestrictInfos.
 *     => Change the hidden attribute exprs to RestrictInfo that the postgres
 *     planner can recognize. However, since the data structure created in HAP
 *     will be used in HAP's partition pruning, the data structure related to
 *     hidden attribute exprs will not be removed.
 */
static bool
hap_propagate_filter_predicates(PlannerInfo *root)
{
	/* If there is no hidden attribute quals, exit immediately */
	if (!hap_propagate_hidden_attribute_exprs(root))
		return false;

	hap_make_restrictinfos_from_hidden_attribute_exprs(root);

	return true;
}

/*
 * hap_propagate_hidden_attribute
 *		Convert the quals in the dimension tables to quals for hidden attribute
 *		and propagate them to the fact tables.
 *
 * We first find the propagation paths. If there is no possible propagation
 * path, there is no need to bear the overhead of finding encoding values and
 * creating hidden attribute quals.
 *
 * If there is a propagation path, find the encoding values and create hidden
 * attribute quals for fact tables.
 *
 * Return true if at least one qual for the hidden attribute has been created
 * and propagated.
 */
bool
hap_propagate_hidden_attribute(PlannerInfo *root)
{
	if (!hap_find_propagation_paths(root))
		return false;

	return hap_propagate_filter_predicates(root);
}

/*
 * hap_check_dimension_table_existence
 * 		Check if there are any dimension tables that can create hidden attribute
 *		quals.
 *
 * Dimension tables encode frequently used attributes in queires as hidden
 * attribute. Find out if simple_rel_array has such a dimension table.
 *
 * Note that we do not acquire the encoding values, only the index of the
 * dimension tables is remembered. This is because getting the encoding value is
 * heavy, so we want to get them after finding the propagation path (if there is
 * no propagation path, we don't have to get the encoding values).
 */
bool
hap_check_dimension_table_existence(PlannerInfo *root)
{
	int rti;

	/* If there is no JOIN, we don't have to use hidden attribute */
	if (root->simple_rel_array_size < 2)
		return false;

	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RangeTblEntry *rte = root->simple_rte_array[rti];

		if (HapRelidIsEncoded(rte->relid))
			return true;
	}

	return false;
}

/*
 * Deep copy the given HapJoinInfoBoolIndicator list.
 */
static List *
hap_deep_copy_bool_indicator_list(List *bool_indicator_list)
{
	List *new_list = NIL;
	ListCell *lc;

	Assert(bool_indicator_list != NIL);

	foreach(lc, bool_indicator_list)
	{
		HapJoinInfoBoolIndicator *indicator
			= (HapJoinInfoBoolIndicator *) lfirst(lc);
		HapJoinInfoBoolIndicator *copy
			= palloc0(sizeof(HapJoinInfoBoolIndicator));

		memcpy(copy, indicator, sizeof(HapJoinInfoBoolIndicator));
		new_list = lappend(new_list, copy);
	}

	return new_list;
}

/*
 * hap_deconstruct_joininfo_for_propagating_internal
 *		Analyze non-boolean clause.
 *
 * We analyze this clause for two purpose.
 *
 * 1. Find a clause which uses the given relation and remember its location.
 *
 * 2. Check whether the given clause can be represented to hidden attribute
 *    clause. If not, we should not propagate the joininfo. 
 *
 * Note that if we decided to remember its location, we have to copy the
 * bool_indicator_list to the expr_location_list. Because the list is
 * continously used during the recursive function calls.
 */
static bool
hap_deconstruct_joininfo_for_propagating_internal(PlannerInfo *root,
												  RelOptInfo *rel,
												  Expr *expr,
												  Expr *original_joininfo_expr,
												  List *bool_indicator_list,
												  List **expr_location_list)
{
	Bitmapset *varnos = NULL, *varattnos = NULL;
	AttrNumber attrno;
	bool encoded;
	Index varno;
	Oid relid;

	/* Get the relation index of this clause */
	varnos = pull_varnos(root, (Node *) expr);

	/* XXX Should we consider multiple var numbers in non-bool clause? */
	Assert(bms_num_members(varnos) == 1);
	varno = bms_first_member(varnos);

	/* Get the attribute number */
	pull_varattnos((Node *) expr, varno, &varattnos);

	/* XXX Should we consider multiple attributes? */
	Assert(bms_num_members(varattnos) == 1);
	attrno = bms_first_member(varattnos) + FirstLowInvalidHeapAttributeNumber;

	/* Check whether this attribute is encoded or not */
	relid = root->simple_rte_array[varno]->relid;
	encoded = HapGetEncodeTableId(relid, attrno);

	/* If this clause targets the given rel, remember it */
	if (encoded && rel->relid == varno)
	{
		HapJoinInfoExprLocation *location
			= palloc0(sizeof(HapJoinInfoExprLocation));

		location->original_joininfo_expr = original_joininfo_expr;
		location->target_expr = expr;
		location->bool_indicator_list
			= hap_deep_copy_bool_indicator_list(bool_indicator_list);

		*expr_location_list = lappend(*expr_location_list, location);
	}

	return encoded;
}

/*
 * hap_deconstruct_joininfo_for_propagating_sub
 *		Analyze BoolExpr in joininfo recursively.
 *
 * Note that the indicator for the topmost OR clause has already been provided
 * in the first element of the bool_indicator_list.  
 */
static bool
hap_deconstruct_joininfo_for_propagating_sub(PlannerInfo *root,
											 RelOptInfo *rel,
											 Expr *expr,
											 Expr *original_joininfo_expr,
											 List *bool_indicator_list,
											 List **expr_location_list)
{
	HapJoinInfoBoolIndicator *bool_indicator;
	bool all_encoded = true;
	ListCell *lc;

	check_stack_depth();

	Assert(bool_indicator_list != NIL);

	if (!IsA(expr, BoolExpr))
		return hap_deconstruct_joininfo_for_propagating_internal(root, rel, expr,
																 original_joininfo_expr,
																 bool_indicator_list,
																 expr_location_list);

	/*
	 * If we meet a clause which use the given rel, the bool_indicator_list is
	 * fully copied and stored to the *expr_location_list. 
	 *
	 * So we does not have to allocate HapJoinInfoBoolIndicator for each
	 * argument of BoolExpr. We allocate it only once per each depth.
	 */
	bool_indicator = palloc0(sizeof(HapJoinInfoBoolIndicator));
	bool_indicator->index = 0;

	if (is_orclause(expr))
		bool_indicator->booltype = OR_EXPR;
	else if (is_andclause(expr))
		bool_indicator->booltype = AND_EXPR;
	else
		Assert(true); /* XXX Should we consider NOT clause here? */

	bool_indicator_list = lappend(bool_indicator_list, bool_indicator);

	foreach(lc, ((BoolExpr *) expr)->args)
	{
		Expr *arg = (Expr *) lfirst(lc);

		/* Deconstruct recursively */
		all_encoded
			&= hap_deconstruct_joininfo_for_propagating_sub(root, rel, arg,
															original_joininfo_expr,
															bool_indicator_list,
															expr_location_list);

		/* There is at least one attribute that cannot be encoded */
		if (!all_encoded)
			break;

		bool_indicator->index++;
	}

	/*
	 * Now we back to the previous depth.
	 * Free the last HapJoinInfoBoolIndicator allocated by us.
	 */
	bool_indicator_list = list_delete_last(bool_indicator_list);
	Assert(bool_indicator_list != NIL);

	return all_encoded;
}

/*
 * hap_deconstruct_joininfo_for_propagating
 *		Analyze joininfo to check it can be propagated.
 *
 * The given joininfo_expr represents root OR of the expression tree. We analyze
 * its arguments recursively and finds claueses corresponding the given rel and
 * remember the location of them.
 *
 * Note that we can propagate joininfo when the all clauses are encodable to
 * hidden attribute check it too.
 */
static List *
hap_deconstruct_joininfo_for_propagating(PlannerInfo *root,
										 RelOptInfo *rel,
										 Expr *joininfo_expr)
{
	HapJoinInfoBoolIndicator *bool_indicator;
	List *bool_indicator_list = NIL;
	List *expr_location_list = NIL;
	bool can_propagate = true;
	ListCell *lc;

	/* It must OR clause */
	Assert(is_orclause(joininfo_expr));

	/* The top of joininfo's expression tree is OR */
	bool_indicator 	= palloc0(sizeof(HapJoinInfoBoolIndicator));
	bool_indicator->booltype = OR_EXPR;
	bool_indicator->index = 0;
	bool_indicator_list = lappend(bool_indicator_list, bool_indicator);

	foreach(lc, ((BoolExpr *) joininfo_expr)->args)
	{
		Expr *arg = (Expr *) lfirst(lc);

		/* Deconstruct recursively */
		can_propagate
			&= hap_deconstruct_joininfo_for_propagating_sub(root, rel, arg,
															joininfo_expr,
														    bool_indicator_list,
														    &expr_location_list);

		/* There is at least one attribute that cannot be encoded */
		if (!can_propagate)
			break;

		/*
		 * extract_restriction_or_clauses() guarantees that each argument of the
		 * first OR clause of joininfo must has clause corresponding the given rel.
		 */
		Assert(expr_location_list != NIL);

		bool_indicator->index++;
	}

	return can_propagate ? expr_location_list : NIL;
}

/*
 * hap_consider_propagating_joininfo
 *		Check whether the new OR clause can be propagated or not.
 *
 * We need to determine if all attributes in the original clause can be encoded
 * as hidden attribute. If so, the new OR clause itself cannot be propagated.
 * For example, when given the condition
 *		(A.id = 1 AND B.id = 1) OR (A.id = 2 AND B.id = 2),
 * the newly extracted OR clause for A becomes
 *		(A.id = 1 OR A.id = 2).
 *
 * However, if this is propagated, B is not considered, resulting in
 * incomplete filtering. In such cases, the original clause should be propagated
 * to achieve comprehensive filtering.
 * 
 * If it is determined that the original clause should be propagated, set a flag
 * in the new OR clause (in the above case, (A.id = 1 OR A.id = 2)) to indicate
 * that it should not be propagated.
 */
void
hap_consider_propagating_joininfo(PlannerInfo *root, RelOptInfo *rel,
								  Expr *joininfo_expr)
{
	RestrictInfo *new_or_rinfo;
	List *expr_location_list;
	HapJoinInfoMap *map;

	/* If it cannot be propagated, do nothing */
	expr_location_list = hap_deconstruct_joininfo_for_propagating(root, rel,
																  joininfo_expr);
	if (expr_location_list == NIL)
		return;
	
	map = palloc0(sizeof(HapJoinInfoMap));
	map->expr_location_list = expr_location_list;

	rel->hap_propagating_joininfo_maps
		= lappend(rel->hap_propagating_joininfo_maps, map);

	/* Set a flag to notice the new OR clause should not be propagated */
	new_or_rinfo = (RestrictInfo *) llast(rel->baserestrictinfo);
	new_or_rinfo->hap_info_do_not_propagate = true;
}
#else /* !LOCATOR */
/*
 * Dummy function.
 */
bool
hap_check_dimension_table_existence(PlannerInfo *root)
{
	return false;
}

/*
 * Dummy function.
 */
bool
hap_propagate_hidden_attribute(PlannerInfo *root)
{
	return false;
}

/*
 * Dummy function.
 */
void
hap_consider_propagating_joininfo(PlannerInfo *root, RelOptInfo *rel,
								  Expr *joininfo_expr)
{
	return;
}
#endif /* LOCATOR */
