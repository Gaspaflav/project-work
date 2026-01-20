"""
Adaptive Solver for Gold Collection Problem
Implements hybrid GA/Hill Climbing approach with density-based algorithm selection
"""

import random
import networkx as nx


def calculate_full_path_cost_final(problem_instance, path, trip_counts=None):
    """Calculate total path cost considering weight accumulation and trip distribution"""
    total_cost = 0.0
    current_weight = 0.0
    infeasible_nodes = []
    
    dist_dict = problem_instance.dist_dict
    gold_dict = problem_instance.gold_dict
    
    total_trips_in_path = 0
    for i in range(len(path) - 1):
        if path[i+1][0] == 0:
            total_trips_in_path += 1
    
    if trip_counts is None:
        trip_counts = [1] * total_trips_in_path
    
    trip_index = 0
    current_trip_count = trip_counts[trip_index] if trip_index < len(trip_counts) else 1
    
    for i in range(len(path) - 1):
        u = path[i][0]
        v = path[i + 1][0]
        u_flag = path[i][1]
        
        this_segment_trips = current_trip_count

        if u != 0 and u_flag == True:
            gold_at_u = gold_dict.get(u, 0)
            current_weight += gold_at_u / this_segment_trips
        
        dist_uv = dist_dict.get((min(u, v), max(u, v)))
        if dist_uv is None:
            dist_uv = 1.0
        
        cost_segment_unit = dist_uv + (problem_instance._alpha * dist_uv * current_weight) ** problem_instance._beta
        total_cost += cost_segment_unit * this_segment_trips

        if v == 0:
            current_weight = 0.0
            trip_index += 1
            if trip_index < len(trip_counts):
                current_trip_count = trip_counts[trip_index]
            else:
                current_trip_count = 1
        
        if u == 0:
            current_weight = 0.0
    
    return (len(infeasible_nodes), total_cost)


def build_nearest_neighbors_cache(P, k=5):
    """Build cache of k-nearest neighbors per node for efficient mutation"""
    dist_dict = P.dist_dict
    graph = P.graph
    distance_near_cache = {}
    
    for node in graph.nodes():
        neighbors_with_dist = []
        for (u, v), dist in dist_dict.items():
            if u == node:
                neighbors_with_dist.append((v, dist))
            elif v == node:
                neighbors_with_dist.append((u, dist))
        
        neighbors_with_dist.sort(key=lambda x: x[1])
        distance_near_cache[node] = {
            neighbor: dist for neighbor, dist in neighbors_with_dist[:k]
        }
    
    return distance_near_cache


def neighborhood_greedy_strategy_dijistra(problem_instance):
    """Compute shortest Dijkstra paths from depot to all nodes"""
    graph = problem_instance.graph
    node_to_path = dict(nx.single_source_dijkstra_path(graph, source=0, weight='dist'))
    path_list = [node_to_path[node] for node in sorted(node_to_path.keys())]
    return path_list


def choice_a_path(path_list, seed=42):
    """Build path by randomly selecting unvisited nodes"""
    full_path = []
    rng = random.Random(seed)
    ungolden_nodes = [i for i in range(1, len(path_list))]
    
    while ungolden_nodes:
        node = rng.choice(ungolden_nodes)
        path_for_node = list(path_list[node].copy())
        
        for p in path_for_node[:-1]:
            full_path.append((p, False))
        
        path_for_node.reverse()

        for p in (path_for_node[:-1]):
            if p in ungolden_nodes:
                full_path.append((p, True))
                ungolden_nodes.remove(p)
            else:
                full_path.append((p, False))
    
    full_path.append((0, False))
    return full_path


def create_population(problem_instance, population_size):
    """Create initial population using greedy approach"""
    population = []
    near_list = neighborhood_greedy_strategy_dijistra(problem_instance=problem_instance)
    if problem_instance._density < 2:
        for n in range(population_size):
            individual = choice_a_path(near_list, seed=n)
            population.append(individual)
        return population
    else:
        return population


def find_node_with_flag_true(path, wanted_node):
    """Find index of node with active flag in path"""
    for i in range(len(path)):
        if path[i][1] and path[i][0] == wanted_node:
            return i
    return -1


def founding_start_and_end_index(path, start_index):
    """Find trip boundaries: start and end indices where node==0"""
    end_index = start_index + 1
    while start_index > 0 and path[start_index][0] != 0:
        start_index -= 1
    while end_index < len(path) - 1 and path[end_index][0] != 0:
        end_index += 1
    return start_index, end_index


def find_True_flags(path_segment):
    """Extract all nodes with active flag (True) from segment"""
    gold_elements = []
    for node, flag in path_segment:
        if flag:
            gold_elements.append(node)
    return gold_elements


def remove_more_gold_nodes(parent_path, gold_nodes, start_index_2, end_index_2):
    """Remove gold node from path and update boundary indices"""
    new_path = parent_path.copy()
    index = find_node_with_flag_true(new_path, gold_nodes)
    
    start_index, end_index = founding_start_and_end_index(new_path, index)
    list_of_gold_nodes_in_segment = find_True_flags(new_path[start_index:end_index])
    
    if len(list_of_gold_nodes_in_segment) == 1:
        new_path = new_path[:start_index] + new_path[end_index:]
        delta = end_index - start_index
        start_index_2 = start_index_2 - delta if start_index_2 > start_index else start_index_2
        end_index_2 = end_index_2 - delta if end_index_2 > end_index else end_index_2
        return new_path, start_index_2, end_index_2
    
    new_path[index] = (new_path[index][0], False)
    return new_path, start_index_2, end_index_2


def insert_more_gold_nodes(gold_nodes, path_list):
    """Build path for specific gold nodes using greedy strategy"""
    full_path = []
    seed = random.randint(0, 1000)
    rng = random.Random(seed)
    ungolden_nodes = gold_nodes.copy()
    
    while ungolden_nodes:
        node = rng.choice(ungolden_nodes)
        path_for_node = list(path_list[node].copy())
        
        for p in path_for_node[:-1]:
            full_path.append((p, False))
        
        path_for_node.reverse()
        
        for p in (path_for_node[:-1]):
            if p in ungolden_nodes:
                full_path.append((p, True))
                ungolden_nodes.remove(p)
            else:
                full_path.append((p, False))
    
    full_path.append((0, False))
    return full_path


def apply_insertion(segment, idx_rel, new_node, graph):
    """Insert new_node into segment after node at position idx_rel"""
    current_node = segment[idx_rel][0]
    path_link = nx.shortest_path(graph, source=current_node, target=new_node, weight='dist')
    intermediate_nodes = [(n, False) for n in path_link[1:-1]]
    new_segment = segment[:idx_rel + 1] + intermediate_nodes + [(new_node, True)] + segment[idx_rel + 1:]
    return new_segment


def smart_concatenate(*segments):
    """Smart concatenation: avoid consecutive depots during merge
    If segment ends with (0, False) and next starts with (0, False), remove duplicate
    Otherwise ensure depot separator (0, False) between segments"""
    if not segments:
        return []
    
    result = list(segments[0]) if segments[0] else []
    
    for i in range(1, len(segments)):
        segment = segments[i] if segments[i] else []
        
        if not segment:
            continue
        
        if result:
            # Check junction points
            result_ends_depot = result[-1][0] == 0
            segment_starts_depot = segment[0][0] == 0
            
            if result_ends_depot and segment_starts_depot:
                # Both have depot: remove duplicate, keep only one
                result.extend(segment[1:])
            elif result_ends_depot or segment_starts_depot:
                # One has depot: just concatenate
                result.extend(segment)
            else:
                # Neither has depot: add separator (0, False)
                result.append((0, False))
                result.extend(segment)
        else:
            result.extend(segment)
    
    return result


def mutation_neighbor_of_next_insertion_only(path, problem_instance, path_list, neighbor_distance_cache, graph):
    """Core mutation operator: insert k-nearest neighbor into random segment"""
    new_path = path.copy()
    
    random_index = random.randint(1, len(new_path) - 2)
    start_index, end_index = founding_start_and_end_index(new_path, random_index)
    starting_segment = path[start_index:end_index + 1]
    
    idx_to_mutate = None
    for i in range(start_index, end_index):
        if path[i][1]:
            idx_to_mutate = i
            break
    
    if idx_to_mutate is None:
        return path, (0, 0)
    
    next_node = new_path[idx_to_mutate + 1][0]
    current_node = new_path[idx_to_mutate][0]
    
    cached_neighbors = neighbor_distance_cache.get(current_node, {})
    if not cached_neighbors:
        return path, (0, 0)
    
    active_nodes_in_segment = {node for node, flag in path[start_index:end_index + 1] if flag}
    
    candidates = []
    for neighbor in cached_neighbors.keys():
        if neighbor != 0 and neighbor != current_node and neighbor not in active_nodes_in_segment:
            candidates.append(neighbor)
    
    if not candidates:
        return path, (0, 0)
    
    new_node = random.choice(candidates)
    
    new_segment = apply_insertion(
        starting_segment, 
        idx_to_mutate - start_index,
        new_node, 
        graph
    )
    
    new_path = new_path[:start_index] + new_segment + new_path[end_index + 1:]
    
    new_segment_end = start_index + len(new_segment) - 1
    segments_to_remove = []
    
    i = 0
    while i < len(new_path):
        if new_path[i][0] == new_node and new_path[i][1]:
            if i < start_index or i > new_segment_end:
                new_path[i] = (new_node, False)
                
                if i < len(new_path) - 1:
                    dup_seg_start, dup_seg_end = founding_start_and_end_index(new_path, i)
                    has_active_nodes = any(new_path[j][1] for j in range(dup_seg_start, min(dup_seg_end + 1, len(new_path))))
                    
                    if not has_active_nodes:
                        segments_to_remove.append((dup_seg_start, dup_seg_end))
                
                break
        
        i += 1
    
    if segments_to_remove:
        seg_start, seg_end = segments_to_remove[0]
        new_path = new_path[:seg_start] + new_path[seg_end:]
    else:
        first_index_true = None
        last_index_true = None
        first_element_true = None
        last_element_true = None
        
        for i in range(start_index, end_index + 1):
            if new_path[i][1]:
                if first_index_true is None:
                    first_index_true = i
                    first_element_true = new_path[i][0]
                last_index_true = i
                last_element_true = new_path[i][0]
        
        if first_element_true is not None:
            seg = []
            
            path_to_first = path_list[first_element_true]
            for node in path_to_first[:-1]:
                seg.append((node, False))
            seg.append((first_element_true, True))
            
            for i in range(first_index_true + 1, last_index_true):
                seg.append(new_path[i])
            
            if first_element_true != last_element_true:
                seg.append((last_element_true, True))
            
            path_from_last = path_list[last_element_true]
            path_from_last_reversed = list(reversed(path_from_last))
            for node in path_from_last_reversed[1:-1]:
                seg.append((node, False))
            seg.append((0, False))
            
            new_path = smart_concatenate(new_path[:start_index], seg, new_path[end_index + 1:])
        
    infeas_before, cost_before = calculate_full_path_cost_final(problem_instance, path)
    infeas_after, cost_after = calculate_full_path_cost_final(problem_instance, new_path)
    
    delta_infeas = infeas_after - infeas_before
    delta_cost = cost_after - cost_before
    
    return new_path, (delta_infeas, delta_cost)


def mutate_trip_counts(trip_counts):
    """Multiply selected trip count by 4 to test cost reduction"""
    new_counts = trip_counts[:]
    idx_to_change = random.randint(0, len(new_counts) - 1)
    increment = 4
    new_counts[idx_to_change] *= increment
    return new_counts, idx_to_change


def get_trip_boundaries(path, cache=None):
    """Return trip segment boundaries"""
    if cache is not None:
        return cache.get_trip_boundaries()
    
    boundaries = []
    trip_start = 0
    
    # Mark (start, end) indices for each trip (returns to depot at node 0)
    for i in range(len(path) - 1):  # Escludi l'ultimo elemento (depot finale)
        if path[i+1][0] == 0:  # Return to depot
            boundaries.append((trip_start, i+1))
            trip_start = i + 1
    
    return boundaries


def evaluate_trip_mutation_smart(problem_instance, path, old_counts, new_counts, changed_idx):
    """Evaluate cost change for modified trip count on segment only"""
    boundaries = get_trip_boundaries(path)
    start_idx, end_idx = boundaries[changed_idx]
    
    mini_path = path[start_idx : end_idx + 1]
    
    old_trip_count_list = [old_counts[changed_idx]]
    new_trip_count_list = [new_counts[changed_idx]]
    
    _, cost_old = calculate_full_path_cost_final(problem_instance, mini_path, old_trip_count_list)
    _, cost_new = calculate_full_path_cost_final(problem_instance, mini_path, new_trip_count_list)
    
    delta = cost_new - cost_old
    return delta


def crossover_zero_paths_with_delta(parent1, parent2, possible_paths, problem_instance):
    """Crossover operator: replace parent2 segment with parent1 segment"""
    p2_working = parent2.copy()
    
    start_index_1 = random.randint(1, len(parent1) - 2)
    start_index_1, end_index_1 = founding_start_and_end_index(parent1, start_index_1)
    
    segment_p1 = parent1[start_index_1 : end_index_1 + 1]
    
    gold_element = 0
    index_gold_element_1 = -1
    for i in range(start_index_1, end_index_1):
        if parent1[i][1] == True:
            gold_element = parent1[i][0]
            index_gold_element_1 = i
            break
            
    if gold_element == 0:
        return p2_working, (0, 0, [])

    list_gold_1 = find_True_flags(parent1[index_gold_element_1+1 : end_index_1])

    index_gold_element_2 = find_node_with_flag_true(p2_working, gold_element)
    
    try:
        start_index_2, end_index_2 = founding_start_and_end_index(p2_working, index_gold_element_2)
    except (IndexError, TypeError):
        return p2_working, (0, 0, [])

    segment_p2_original = p2_working[start_index_2 : end_index_2 + 1]
    
    res_seg1 = calculate_full_path_cost_final(problem_instance, segment_p1)
    res_seg2 = calculate_full_path_cost_final(problem_instance, segment_p2_original)
    
    delta_infeas = res_seg1[0] - res_seg2[0]
    delta_cost = res_seg1[1] - res_seg2[1]
    infeas_nodes = []
    if len(res_seg1) > 2: 
        infeas_nodes.extend(res_seg1[2])

    list_gold_2 = find_True_flags(p2_working[start_index_2 : end_index_2])
    
    if len(list_gold_1) > 0:
        for node in list_gold_1:
            if node not in list_gold_2:
                idx_external = find_node_with_flag_true(p2_working, node)
                
                if idx_external != -1:
                    s_ext, e_ext = founding_start_and_end_index(p2_working, idx_external)
                    seg_ext_old = p2_working[s_ext : e_ext + 1]
                    res_ext_old = calculate_full_path_cost_final(problem_instance, seg_ext_old)
                    
                    p2_working, start_index_2, end_index_2 = remove_more_gold_nodes(
                        p2_working, node, start_index_2, end_index_2
                    )
                    
                    check_idx = min(idx_external, len(p2_working)-2)
                    s_ext_new, e_ext_new = founding_start_and_end_index(p2_working, check_idx)
                    seg_ext_new = p2_working[s_ext_new : e_ext_new + 1]
                    res_ext_new = calculate_full_path_cost_final(problem_instance, seg_ext_new)
                    
                    delta_infeas += (res_ext_new[0] - res_ext_old[0])
                    delta_cost += (res_ext_new[1] - res_ext_old[1])
                    if len(res_ext_new) > 2: 
                        infeas_nodes.extend(res_ext_new[2])

    if gold_element in list_gold_2:
        list_gold_2.remove(gold_element)
    
    list_2_not_in_1 = [node for node in list_gold_2 if node not in list_gold_1]

    new_path_append = []
    if len(list_2_not_in_1) > 0:
        new_path_append = insert_more_gold_nodes(list_2_not_in_1, possible_paths)
        res_append = calculate_full_path_cost_final(problem_instance, new_path_append)
        
        delta_infeas += res_append[0]
        delta_cost += res_append[1]
        if len(res_append) > 2: 
            infeas_nodes.extend(res_append[2])

    # Construct new individual using smart concatenation to avoid consecutive depots
    new_individual = smart_concatenate(
        p2_working[:start_index_2],
        segment_p1,
        p2_working[end_index_2+1:],
        new_path_append[1:] if new_path_append else []
    )
    
    return new_individual, (delta_infeas, delta_cost, infeas_nodes)


def tournament_selection(population_with_fitness, tournament_size=3):
    """Select best individual from random tournament"""
    candidates = random.sample(population_with_fitness, tournament_size)
    winner = min(candidates, key=lambda x: (x[1][0], x[1][1]))
    return winner[0], winner[1]


def hill_climbing(problem_instance, initial_solution, n_iterations=1000):
    """Hill climbing optimizer: iteratively improve solution by accepting only better moves"""
    current_solution = initial_solution.copy()
    
    curr_infeas, curr_cost = calculate_full_path_cost_final(problem_instance, current_solution)
    
    neighbor_distance_cache = build_nearest_neighbors_cache(problem_instance, k=5)
    path_list = neighborhood_greedy_strategy_dijistra(problem_instance)
    graph = problem_instance.graph
    
    for iteration in range(n_iterations):
        neighbor_solution, delta_tuple = mutation_neighbor_of_next_insertion_only(
            current_solution, problem_instance, path_list, neighbor_distance_cache, graph
        )
        
        delta_infeas = delta_tuple[0]
        delta_cost = delta_tuple[1]
        
        accept = False
        
        if delta_infeas < 0:
            accept = True
        elif delta_infeas > 0:
            accept = False
        else:
            if delta_cost <= 0:
                accept = True
            else:
                accept = False
        
        if accept:
            current_solution = neighbor_solution
            curr_infeas += delta_infeas
            curr_cost += delta_cost

    return current_solution, curr_cost


def genetic_algorithm(problem_instance, population_size=100, n_generations=50, temperature=0.8):
    """Genetic Algorithm with adaptive parameters"""
    neighbor_distance_cache = build_nearest_neighbors_cache(problem_instance, k=5)
    graph = problem_instance.graph 
    
    raw_population = create_population(problem_instance, population_size)
    population_data = []
    
    for ind in raw_population:
        fit = calculate_full_path_cost_final(problem_instance, ind)
        population_data.append((ind, fit))
        
    path_list = neighborhood_greedy_strategy_dijistra(problem_instance)

    def is_better(fit_a, fit_b):
        if fit_a[0] != fit_b[0]:
            return fit_a[0] < fit_b[0]
        return fit_a[1] < fit_b[1]

    for generation in range(n_generations):
        new_population_data = []
        
        progress = generation / n_generations
        
        min_tournament_k = 2
        max_tournament_k = 6
        current_tournament_k = int(min_tournament_k + progress * (max_tournament_k - min_tournament_k))
        current_tournament_k = max(2, min(6, current_tournament_k))
        
        base_crossover_rate = 0.1
        max_added_rate = 0.8
        current_crossover_prob = base_crossover_rate + (progress * temperature * max_added_rate)
        current_crossover_prob = max(0.0, min(1.0, current_crossover_prob))

        best_of_gen = min(population_data, key=lambda x: (x[1][0], x[1][1]))
        new_population_data.append(best_of_gen)
        
        while len(new_population_data) < population_size:
            parent1_path, p1_fit = tournament_selection(population_data, current_tournament_k)
            parent2_path, p2_fit = tournament_selection(population_data, current_tournament_k)
            
            winner_path = None
            winner_fit = None
            
            if random.random() > current_crossover_prob:
                child_path, mut_delta = mutation_neighbor_of_next_insertion_only(
                    parent1_path, problem_instance, path_list, neighbor_distance_cache, graph
                )
                
                m_infeas = p1_fit[0] + mut_delta[0]
                m_cost = p1_fit[1] + mut_delta[1]
                child_fit = (m_infeas, m_cost, [])

                if is_better(child_fit, p1_fit):
                    winner_path, winner_fit = child_path, child_fit
                else:
                    winner_path, winner_fit = parent1_path, p1_fit

            else:
                child_path, delta_tuple = crossover_zero_paths_with_delta(
                    parent1_path, parent2_path, path_list, problem_instance
                )
                
                child_infeas = p2_fit[0] + delta_tuple[0]
                child_cost = p2_fit[1] + delta_tuple[1]
                child_fit = (child_infeas, child_cost, []) 
                
                if is_better(child_fit, p2_fit):
                    winner_path, winner_fit = child_path, child_fit
                else:
                    winner_path, winner_fit = parent2_path, p2_fit
            
            new_population_data.append((winner_path, winner_fit))
        
        population_data = new_population_data

    return min(population_data, key=lambda x: (x[1][0], x[1][1]))


def run_hill_climbing_trips(problem_instance, path, initial_trip_counts, max_iter=1000):
    """Optimize trip counts using hill climbing"""
    current_counts = list(initial_trip_counts)
    _, current_total_cost = calculate_full_path_cost_final(problem_instance, path, current_counts)
    
    iteration = 0
    improvements = 0
    scaler = 8
    
    for i in range(2):
        new_counts = [int(c * scaler) for c in current_counts]
        _, new_cost = calculate_full_path_cost_final(problem_instance, path, new_counts)
        
        if new_cost < current_total_cost:
            current_counts = new_counts
            current_total_cost = new_cost
        else:
            scaler = scaler / 2
        
    while iteration < max_iter:
        iteration += 1
        
        new_counts, changed_idx = mutate_trip_counts(current_counts)
        
        delta = evaluate_trip_mutation_smart(
            problem_instance, 
            path, 
            current_counts,
            new_counts, 
            changed_idx
        )
        
        if delta < 0:
            current_counts = new_counts
            current_total_cost += delta
            improvements += 1

    return current_counts, current_total_cost


def conversion_solution(problem_instance, full_path, trip_counts):
    """Convert internal path representation to output format"""
    converted_path = []
    list_of_bounds = get_trip_boundaries(full_path)
    for trip_idx in range(len(trip_counts)):
        start_idx, end_idx = list_of_bounds[trip_idx]
        trip_count = trip_counts[trip_idx]
        for num_trips in range(trip_count):
            for i in range(start_idx, end_idx):
                node, is_active = full_path[i]
                if is_active:
                    gold_dict = problem_instance.gold_dict 
                    total_gold = gold_dict[node]
                    gold_amount = total_gold / trip_count if trip_count > 0 else 0
                    converted_path.append((node, gold_amount))
                else:
                    converted_path.append((node, 0))

    converted_path.append((0, 0))
    converted_path = converted_path[1:]
    return converted_path


def adaptive_solver(problem_instance, n_trip_count_hc=500, fast=True):
    """
    MAIN ORCHESTRATOR: Density-based algorithm selection
    - Sparse (density < 0.8): Genetic Algorithm with doubled population
    - Dense (density >= 0.8): Hill Climbing from greedy initialization
    - Automatically adjusts parameters based on problem size and beta factor
    """
    
    # Clamp problem size to reasonable bounds
    n = len(problem_instance.graph.nodes())
    n = max(100, min(n, 1000))

    # Compute adaptive parameters based on problem size
    population_size = int((30 - (n - 100) / 60) / 2)
    n_generations = int(80 - (n - 100) / 18)
    n_trip_count_hc = int(n)

    if n >= 800:
        n_trip_count_hc = n_trip_count_hc / 2

    # Double population for sparse graphs (more diversity needed for exploration)
    if problem_instance._density < 0.8:
        population_size *= 2

    # Reduce generations for large, dense, high-beta problems (faster convergence)
    if n > 500 and fast and problem_instance._beta > 1:
        n_generations = int(n_generations / 2)

    # DENSITY-BASED ALGORITHM SELECTION
    if problem_instance._density < 0.8:
        # Sparse: GA explores diverse solution space effectively
        ga_result = genetic_algorithm(problem_instance, population_size, n_generations)
        best_path = ga_result[0]
    else:
        # Dense: HC with greedy initialization converges faster than diverse GA
        path_list = neighborhood_greedy_strategy_dijistra(problem_instance)
        initial_path = choice_a_path(path_list, seed=42)
        best_path, _ = hill_climbing(problem_instance, initial_path, n_iterations=population_size * n_generations)

    # TRIP OPTIMIZATION: Divide gold across multiple trips to reduce weight penalty
    # Only optimizes when beta > 1 (weight penalty is significant)
    initial_trip_counts = [1 for _ in range(len(get_trip_boundaries(best_path)))]
    
    if problem_instance._beta > 1:
        # Hill climbing on trip counts: multiply counts to reduce total weight penalty
        optimized_trip_counts, final_cost = run_hill_climbing_trips(
            problem_instance, 
            best_path, 
            initial_trip_counts, 
            max_iter=int(n_trip_count_hc)
        )
    else:
        # Beta <= 1: no weight penalty benefit from multiple trips
        optimized_trip_counts = initial_trip_counts
        _, final_cost = calculate_full_path_cost_final(problem_instance, best_path, optimized_trip_counts)
    
    return best_path, optimized_trip_counts, final_cost
