/*-------------------------------------------------------------------------
 *
 * hap_planner.h
 *	  POSTGRES hidden attribute partitioning (HAP) planner.
 *
 *
 * src/include/locator/hap/hap_planner.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HAP_PLANNER_H
#define HAP_PLANNER_H

#include "nodes/nodes.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"

#include "locator/hap/hap_encoding.h"
#include "locator/hap/hap_reduction.h"

/*
 * HapPropagatePath - The path of hidden attribute propagation.
 *
 * - rel_oid_list: Relation oids passed through in the propagation.
 * - target_rel_idx: Target RelOptInfo index to be propagated to.
 */
typedef struct HapPropagatePath
{
	List *rel_oid_list;
	Index target_rel_idx;
} HapPropagatePath;

/*
 * HapHiddenAttrOpExpr - "=" operation for hidden attribute.
 *
 * - expr: Expression node for an operator invocation.
 * - desc: Hidden attribute descriptor. 
 * - value: Encoding value.
 */
typedef struct HapHiddenAttrOpExpr
{
	OpExpr expr;
	HapHiddenAttrDesc *desc;
	int value;
	Selectivity selec;
} HapHiddenAttrOpExpr;

/*
 * HapHiddenAttrScalarArrayOpExpr - "IN (array)" operation for hidden attribute.
 *
 * - expr: Expression node for "scalar op ANY/ALL (array)".
 * - desc: Hidden attribute descriptor.
 * - value_list: Encoding value list.
 */
typedef struct HapHiddenAttrScalarArrayOpExpr
{
	ScalarArrayOpExpr expr;
	HapHiddenAttrDesc *desc;
	List *value_list;
	Selectivity selec;
} HapHiddenAttrScalarArrayOpExpr;

/*
 * HapHiddenAttrBoolExpr - "AND/OR" operation for hidden attribute.
 *
 * - expr: Expression node for the basic Boolean operators AND, OR, NOT.
 * - args: Arguments to this expression.
 */
typedef struct HapHiddenAttrBoolExpr
{
	BoolExpr expr;
	List *args;
	Selectivity selec;
} HapHiddenAttrBoolExpr;

/*
 * HapJoinInfoBoolIndicator - Indicating the position of an argument in booltype.
 *
 * - booltype: Type indicating AND/OR.
 * - index: Position of an argument.
 */
typedef struct HapJoinInfoBoolIndicator
{
	BoolExprType booltype;
	int index;
} HapJoinInfoBoolIndicator;

/*
 * HapJoinInfoExprLocation - Indicating the location of expr.
 *
 * original_joininfo_clause: Target joininfo.
 * target_expr: Target expr.
 * bool_indicator_list: List of HapJoinInfoBoolIndicator
 */
typedef struct HapJoinInfoExprLocation
{
	Expr *original_joininfo_expr;
	Expr *target_expr;
	List *bool_indicator_list;
} HapJoinInfoExprLocation;

/*
 * HapJoinInfoMap - Indicating the clauses in the joininfo.
 *
 * expr_location_list: List of HapJoinInfoExprLocation.
 */
typedef struct HapJoinInfoMap
{
	List *expr_location_list;
} HapJoinInfoMap;

/*
 * HapJoinInfoExpr - Multiple hidden attribute opration.
 *
 * expr: Expression node for the OR operation.
 * original_joininfo_expr: Pointing original joininfo clause's address.
 */
typedef struct HapJoinInfoExpr
{
	BoolExpr expr;
	Expr *original_joininfo_expr;
} HapJoinInfoExpr;

extern bool hap_check_dimension_table_existence(PlannerInfo *root);

extern bool hap_propagate_hidden_attribute(PlannerInfo *root);

extern List *hap_get_hidden_attribute_encoding_values(Oid encode_table_id,
													  Expr *expr);

extern void hap_consider_propagating_joininfo(PlannerInfo *root,
											  RelOptInfo *rel,
											  Expr *joininfo_expr);

extern void hap_populate_for_substitution(HapSubstitutionInfo *info, 
											char **data_store, int *data_size);

extern PGDLLIMPORT bool enable_hap_planner;

extern PGDLLIMPORT bool enable_hap_selectivity;

#endif	/* HAP_PLANNER_H */
