import argparse
import copy
import json

from itertools import combinations, chain

def get_subsets(R, projection):
    """
    Generate subsets of R based on the given projection.
    The subsets are divided into those included in the projection and those not.
    """
    in_projection = R.intersection(projection)
    not_in_projection = R.difference(projection)
    subsets = [in_projection] if in_projection else []
    if not_in_projection:
        subsets.append(not_in_projection)
    return subsets

def generate_initial_subsets(R, projections):
    """
    Recursively divide the attribute set R into subsets using the projections.
    """
    subsets = [R]
    for projection in projections:
        new_subsets = []
        for subset in subsets:
            split_subsets = get_subsets(subset, projection)
            new_subsets.extend(split_subsets)
        subsets = new_subsets
    return subsets

def merge_subsets(subsets):
    """
    Given a list of subsets, generate all possible merged combinations, 
    removing duplicates that only differ by order.
    
    :param subsets: List of initial subsets
    :return: List of all possible unique merged subsets combinations
    """
    if not subsets:
        return [[]]

    # Initialize set to hold all possible unique merges
    all_combinations = set()

    # Generate all combinations of merging subsets
    for r in range(1, len(subsets) + 1):
        for combo in combinations(subsets, r):
            # Merging the selected combination
            merged = frozenset().union(*combo)
            remaining_subsets = [s for s in subsets if s not in combo]
            
            # Recursively generate combinations with the remaining subsets
            for next_combo in merge_subsets(remaining_subsets):
                all_combinations.add(frozenset([merged] + [frozenset(s) for s in next_combo]))
    
    # Convert back to a list of lists
    return [list(map(set, combination)) for combination in all_combinations]

def calculate_cost(CG, level, args):
    """
    Calculate the cost of the current CG based on the formula provided.
    See the equation 9 of LASER.
    """
    global T, L, B, c, w
    global point_lookup_projection, range_scan_projection, update_projection

    g_i = sum(len(partition) for partition in CG)  # Total number of attributes in the CG
    term_1 = (w * T * g_i) / (B * c)
    
    p_i = args.num_point_lookups_per_level[level]

    # ex) CG = [{'a1'}, {'a5'}, {'a2', 'a4', 'a3'}]
    # If point lookup requries 'a1' and 'a2',
    # then we need two column groups, {'a1'} and {'a2', 'a4', 'a3'}.
    # => E_ik_g = 2
    E_ik_g = 0
    for cg_subset in CG:
        if point_lookup_projection.intersection(cg_subset):
            E_ik_g += 1

    term_2 = p_i * E_ik_g
    
    q_i = args.num_range_scans_per_level[level]
    s_ik = get_level_selectivity(args, level)

    # ex) CG = [{'a1'}, {'a5'}, {'a2', 'a4', 'a3'}]
    # If range scan (or update) requires 'a1' and 'a2',
    # then we need two column groups, {'a1'} and {'a2', 'a4', 'a3'}.
    # => E_ik_G = (1 + 1) + (1 + 3)
    # => (1 + 1) means (key, 'a1') and
    # (1 + 3) means (key, 'a2', 'a4', 'a3')
    E_ik_G = 0
    for cg_subset in CG:
        if range_scan_projection.intersection(cg_subset):
            cg_size = len(cg_subset)
            E_ik_G += 1 + cg_size # Consider key. Add 1

    term_3 = q_i * ((s_ik * E_ik_G) / (c * B)) 
    
    u_i = args.num_updates_per_level[level]

    E_ik_G = 0
    for cg_subset in CG:
        if update_projection.intersection(cg_subset):
            cg_size = len(cg_subset)
            E_ik_G += 1 + cg_size # Consider key. Add 1

    term_4 = u_i * ((T * E_ik_G) / (c * B))
    
    return term_1 + term_2 + term_3 + term_4

def optimize_CG_level(R, projections, level, args):
    """
    Optimize the CG for a given level based on the workload statistics.
    """
    # Step 1: Generate the initial subsets
    initial_subsets = generate_initial_subsets(R, projections)
    
    # Step 2: Make all possible merging
    all_merged_subsets = merge_subsets(initial_subsets)

    # Step 3: Consider all possible CGs and select the one with the least cost
    min_cost = float('inf')
    best_CG = None
    
    for CG in all_merged_subsets:
        if set().union(*CG) == R:  # Check if partition covers all attributes in R
            cost = calculate_cost(CG, level, args)
            if cost < min_cost:
                min_cost = cost
                best_CG = CG
    
    return best_CG

def get_projections(args):
    """
    Generate projection set list using the given arguments.
    """
    global point_lookup_projection, range_scan_projection, update_projection, join_projection
    last = args.num_attributes # last attribute number

    # Point lookup's projection
    point_lookup_projection = set()
    for i in range(last, last - args.point_lookup_num_projection, -1):
        point_lookup_projection.add(f'a{i}')

    # Range scan's projection
    range_scan_projection = set()
    for i in range(last, last - args.range_scan_num_projection, -1):
        range_scan_projection.add(f'a{i}')

    # Update's projection
    update_projection = set()
    for i in range(last, last - args.update_num_projection, -1):
        update_projection.add(f'a{i}')

    # Join's projection
    join_projection = {'a1'}
    for i in range(2, args.join_num_projection + 1):
        join_projection.add(f'a{i}')

    projections = [
        point_lookup_projection,
        range_scan_projection,
        update_projection
    ]

    if args.join_num_projection != 0:
        # Join's projection
        join_projection = {'a1'}
        for i in range(2, args.join_num_projection + 1):
            join_projection.add(f'a{i}')
        projections.append(join_projection)

    return projections

def get_level_selectivity(args, level):
    """
    In Laser, the term "selectivity" is used differently from its general usage;
    it refers to the number of keys selected. For example, in a range scan that
    requests keys in the range of 1 to 100, the selectivity is 100.

    Laser assumes that keys are uniformly distributed across all levels.
    Given this assumption, calculate the selectivity for a given level.
    """
    return args.range_scan_num_selected_keys / (args.level_size_ratio ** (args.num_levels - level))

def generate_CG_matrix(best_CG_list_level, args):
    """
    Create a CG matrix that stores the best CG at each level and save it
    as a file, so that it can be recognized by the Laser.
    """
    processed_data = [
        sorted(
            [(min(int(s[1:]) - 1 for s in subset), max(int(s[1:]) - 1 for s in subset))
                for subset in sublist],
            key=lambda t: t[0]  # sort by t[0]
        )
    for sublist in best_CG_list_level ]

    with open(args.cg_matrix_file_name, "w") as f:
        for sublist in processed_data:
            f.write(",".join(f"{t[0]} {t[1]}" for t in sublist) + "\n")

def main():
    parser = argparse.ArgumentParser(description="Find optimal column group.")

    parser.add_argument('--num-levels', type=int, required=True, help="Total number of levels")
    parser.add_argument('--level-size-ratio', type=int, required=False, default=2, help="Size ration between adjacent levels")
    parser.add_argument('--num-attributes', type=int, required=True, help="Number of additional attributes except primary key and foreign key")
    parser.add_argument('--point-lookup-num-projection', type=int, required=True, help="Number of attributes projected in point lookup")
    parser.add_argument('--range-scan-num-selected-keys', type=int, required=True, help="Range scan selectivity (number of entries selected)")
    parser.add_argument('--range-scan-num-projection', type=int, required=True, help="Number of attributes projected in range scan")
    parser.add_argument('--update-num-projection', type=int, required=True, help="Number of attributes projected in update")
    parser.add_argument('--join-num-projection', type=int, required=True, help="Number of attributes projected in join")
    parser.add_argument('--num-point-lookups-per-level', type=lambda s: [int(item) for item in s.split(',')], required=True, help="Number of point lookups per level")
    parser.add_argument('--num-range-scans-per-level', type=lambda s: [int(item) for item in s.split(',')], required=True, help="Number of point lookups per level")
    parser.add_argument('--num-updates-per-level', type=lambda s: [int(item) for item in s.split(',')], required=True, help="Number of point lookups per level")
    parser.add_argument('--num-inserts', type=int, required=True, help="Number of inserts")
    parser.add_argument('--cg-matrix-file-name', type=str, required=False, default="cg_matrix", help="Path where CG matrix will be saved")

    args = parser.parse_args()

    page_size = 4096 # 4KB

    # The size of primary key is 8byte.
    # The size of additional attributes is 4byte.
    record_size = 8 + args.num_attributes * 4

    # Define constants
    # Refer to Table 1 of Laser for the explanation of variable names.
    global T, L, B, c, w
    T = args.level_size_ratio
    L = args.num_levels
    c = 1 + args.num_attributes
    B = page_size / record_size
    w = args.num_inserts

    R = {'a1'} # a1 is foreign key. Always exists.

    # Create the entire attribute set according to the schema defined in
    # build.py
    for i in range(2, args.num_attributes + 1):
        attribute = f'a{i}'
        R.add(attribute)

    projections = get_projections(args)

    # Level 0 uses row-oriented style.
    best_CG_list_all_level = [ [R], ]
    prev_level_CG_list = [R]
    current_level_CG_list = []

    for level in range(1, args.num_levels):
        # ex) prev_level_CG_list = [ {a1, a2, a3}, {a4, a5} ]
        for CG in prev_level_CG_list:
            # ex) CG = {a1, a2, a3}, split this set.
            best_CG_list = optimize_CG_level(CG, projections, level, args)

            # ex) best_CG_list = [ {a1}, {a2, a3} ]
            for s in best_CG_list:
                current_level_CG_list.append(s)

        best_CG_list_all_level.append(current_level_CG_list)
        prev_level_CG_list = copy.deepcopy(current_level_CG_list)
        current_level_CG_list = []

    generate_CG_matrix(best_CG_list_all_level, args)

if __name__ == "__main__":
    main()
