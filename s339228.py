

from src.solver import adaptive_solver, conversion_solution


def solution(p):

    # Call the adaptive solver to find best path and trip counts
    best_path, optimized_trip_counts, final_cost = adaptive_solver(p)
    
    # Convert internal representation to output format
    converted_solution = conversion_solution(p, best_path, optimized_trip_counts)
    
    return converted_solution
