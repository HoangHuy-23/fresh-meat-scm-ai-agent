import sys
import json
import os
import requests
from dotenv import load_dotenv
from math import radians, sin, cos, sqrt, atan2
from flask import Flask, request, jsonify
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp

# --- LỚP CẤU HÌNH VÀ HELPER ---

# Load biến môi trường từ file .env
load_dotenv()
API_SERVER_URL = os.getenv("API_SERVER_URL", "http://localhost:8080")
API_TOKEN = os.getenv("AGENT_API_TOKEN")

def normalize_quantity_to_kg(item, product_catalog):
    sku = item.get('sku') or item.get('assetID')
    if not sku: return 0
    product_info = product_catalog.get(sku)
    if not product_info:
        print(f"Warning: Product info for SKU '{sku}' not found. Cannot calculate weight.", file=sys.stderr)
        return 0
    quantity_value = item.get('quantity', {}).get('value', 0)
    avg_weight_obj = product_info.get('averageWeight', {})
    avg_weight_value = avg_weight_obj.get('value', 0)
    avg_weight_unit = avg_weight_obj.get('unit', 'kg').lower()
    weight_in_kg = avg_weight_value
    if avg_weight_unit == 'g':
        weight_in_kg = avg_weight_value / 1000.0
    return quantity_value * weight_in_kg

def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1_rad, lon1_rad = radians(lat1), radians(lon1)
    lat2_rad, lon2_rad = radians(lat2), radians(lon2)
    dlon, dlat = lon2_rad - lon1_rad, lat2_rad - lat1_rad
    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

# --- LỚP LOGIC NGHIỆP VỤ (MATCHING) ---

def create_transport_tasks(dispatch_requests, replenishment_requests, all_facilities, product_catalog):
    print("\n--- Starting Task Creation ---", file=sys.stderr)
    tasks = []
    facility_map = {f['facilityID']: f for f in all_facilities}
    
    # Xây dựng kho ảo từ Processor
    available_inventory = {}
    if dispatch_requests:
        for req in dispatch_requests:
            from_facility = facility_map.get(req.get('fromFacilityID'))
            if from_facility and from_facility.get('type') == 'PROCESSOR' and req.get('status') == 'PENDING':
                for item in req.get('items', []):
                    sku = item.get('sku') or item.get('assetID')
                    if sku:
                        if sku not in available_inventory: available_inventory[sku] = []
                        available_inventory[sku].append({
                            "from_facility": req['fromFacilityID'],
                            "quantity_value": item['quantity']['value'],
                            "quantity_unit": item['quantity']['unit'],
                            "original_item": item, "original_request_id": req['requestID']
                        })
    print(f"Available inventory from Processors: {json.dumps(available_inventory, indent=2)}", file=sys.stderr)

    # Ưu tiên 1: Đáp ứng yêu cầu của Retailer
    print("\n--- Phase 1: Matching Retailer Replenishment Requests ---", file=sys.stderr)
    if replenishment_requests:
        for rep_req in replenishment_requests:
            if rep_req.get('status') != 'PENDING': continue
            print(f"Processing Replenishment Request: {rep_req.get('requestID')}", file=sys.stderr)
            for item_needed in rep_req.get('items', []):
                sku_needed = item_needed.get('sku')
                if not sku_needed: continue
                
                needed_value = item_needed['quantity']['value']
                needed_unit = item_needed['quantity']['unit']
                print(f"  - Needs {needed_value} {needed_unit} of SKU {sku_needed}", file=sys.stderr)
                
                # Cố gắng đáp ứng từ kho ảo (Processor)
                if sku_needed in available_inventory:
                    for source in available_inventory[sku_needed]:
                        if needed_value <= 0: break
                        if source['quantity_value'] > 0 and source['quantity_unit'] == needed_unit:
                            take_value = min(needed_value, source['quantity_value'])
                            task_item = {**source['original_item'], 'quantity': {**source['original_item']['quantity'], 'value': take_value}}
                            tasks.append({
                                "from": source['from_facility'], "to": rep_req['requestingFacilityID'], "demand_kg": normalize_quantity_to_kg(task_item, product_catalog),
                                "items": [task_item], "vehicle_type": "COLD_CHAIN", "original_request_ids": {source['original_request_id']}
                            })
                            print(f"      ==> CREATED TASK (from Processor): {source['from_facility']} -> {rep_req['requestingFacilityID']} ({take_value} {needed_unit})", file=sys.stderr)
                            needed_value -= take_value
                            source['quantity_value'] -= take_value
                
                # Nếu vẫn còn cần, tìm trong kho (Warehouse)
                if needed_value > 0:
                    print(f"  - Still needs {needed_value} {needed_unit}. Searching in warehouses...", file=sys.stderr)
                    active_warehouses = [f for f in all_facilities if f.get('type') == 'WAREHOUSE' and f.get('status') == 'ACTIVE']
                    for wh in active_warehouses:
                        if needed_value <= 0: break
                        
                        headers = {'Authorization': f'Bearer {API_TOKEN}'}
                        inventory_resp = requests.get(f"{API_SERVER_URL}/api/v1/facilities/{wh['facilityID']}/inventory?sku={sku_needed}", headers=headers)
                        
                        if inventory_resp.status_code == 200:
                            inventory_in_wh = inventory_resp.json()
                            if not inventory_in_wh:
                                print(f"    > No inventory for SKU {sku_needed} in Warehouse {wh['facilityID']}.", file=sys.stderr)
                                continue 
                            
                            print(f"    > Found {len(inventory_in_wh)} asset(s) for SKU {sku_needed} in Warehouse {wh['facilityID']}.", file=sys.stderr)
                            
                            for asset_in_wh in inventory_in_wh:
                                if needed_value <= 0: break
                                
                                available_value = asset_in_wh.get('currentQuantity', {}).get('value', 0)
                                if available_value > 0:
                                    take_value = min(needed_value, available_value)
                                    
                                    task_item = {
                                        "assetID": asset_in_wh['assetID'],
                                        "sku": sku_needed,
                                        "quantity": {"unit": needed_unit, "value": take_value}
                                    }
                                    
                                    tasks.append({
                                        "from": wh['facilityID'], "to": rep_req['requestingFacilityID'], 
                                        "demand_kg": normalize_quantity_to_kg(task_item, product_catalog),
                                        "items": [task_item], "vehicle_type": "COLD_CHAIN", "original_request_ids": set()
                                    })
                                    print(f"      ==> CREATED TASK (from Warehouse): {wh['facilityID']} -> {rep_req['requestingFacilityID']} ({take_value} {needed_unit}) using Asset {asset_in_wh['assetID']}", file=sys.stderr)
                                    needed_value -= take_value
                        else:
                            print(f"    > WARN: Failed to get inventory for Warehouse {wh['facilityID']}. Status: {inventory_resp.status_code}", file=sys.stderr)

    # Ưu tiên 2: Chuyển hàng dư và hàng thô
    print("\n--- Phase 2: Handling Surplus and Raw Materials ---", file=sys.stderr)
    default_warehouse = next((f['facilityID'] for f in all_facilities if f.get('type') == 'WAREHOUSE' and f.get('status') == 'ACTIVE'), None)
    default_processor = next((f['facilityID'] for f in all_facilities if f.get('type') == 'PROCESSOR' and f.get('status') == 'ACTIVE'), None)

    # Hàng thành phẩm dư từ Processor -> Warehouse
    if available_inventory:
        for sku, sources in available_inventory.items():
            for source in sources:
                if source['quantity_value'] > 0 and default_warehouse:
                    task_item = {**source['original_item'], 'quantity': {**source['original_item']['quantity'], 'value': source['quantity_value']}}
                    tasks.append({
                        "from": source['from_facility'], "to": default_warehouse, "demand_kg": normalize_quantity_to_kg(task_item, product_catalog),
                        "items": [task_item], "vehicle_type": "COLD_CHAIN", "original_request_ids": {source['original_request_id']}
                    })
                    print(f"  ==> CREATED SURPLUS TASK: {source['from_facility']} -> {default_warehouse} ({source['quantity_value']} {source['quantity_unit']})", file=sys.stderr)

    # Hàng thô từ Farm -> Processor
    if dispatch_requests:
        for req in dispatch_requests:
            from_facility = facility_map.get(req.get('fromFacilityID'))
            if from_facility and from_facility.get('type') == 'FARM' and req.get('status') == 'PENDING' and default_processor:
                demand_kg = sum(normalize_quantity_to_kg(item, product_catalog) for item in req.get('items', []))
                tasks.append({
                    "from": req['fromFacilityID'], "to": default_processor, "demand_kg": demand_kg,
                    "items": req['items'], "vehicle_type": "RAW_MATERIAL_TRUCK", "original_request_ids": {req['requestID']}
                })
                print(f"  ==> CREATED RAW MATERIAL TASK: {req['fromFacilityID']} -> {default_processor} ({demand_kg}kg)", file=sys.stderr)
            
    print(f"\n--- Finished Task Creation. Total tasks created: {len(tasks)} ---", file=sys.stderr)
    return tasks

# --- LỚP TỐI ƯU HÓA (VRP SOLVER) ---

def solve_vrp_for_vehicle_type(tasks, vehicles, all_facilities, vehicle_type):
    """Giải bài toán VRP và gom nhóm các điểm dừng một cách chính xác."""
    if not tasks or not vehicles:
        return []

    facility_map = {f['facilityID']: f['address'] for f in all_facilities}
    location_ids = {"DEPOT": 0}
    locations = [{"latitude": 0, "longitude": 0}]
    pickups_deliveries = []
    
    for task in tasks:
        from_facility, to_facility = task['from'], task['to']
        if from_facility not in location_ids:
            location_ids[from_facility] = len(locations)
            locations.append(facility_map[from_facility])
        if to_facility not in location_ids:
            location_ids[to_facility] = len(locations)
            locations.append(facility_map[to_facility])
        pickups_deliveries.append([location_ids[from_facility], location_ids[to_facility]])

    data = {}
    data['distance_matrix'] = [[int(haversine_distance(l1['latitude'], l1['longitude'], l2['latitude'], l2['longitude']) * 100) for l2 in locations] for l1 in locations]
    data['pickups_deliveries'] = pickups_deliveries
    data['num_vehicles'] = len(vehicles)
    data['vehicle_capacities'] = [int(v['specs']['payloadTonnes'] * 1000) for v in vehicles]
    data['depot'] = 0
    
    manager = pywrapcp.RoutingIndexManager(len(data['distance_matrix']), data['num_vehicles'], data['depot'])
    routing = pywrapcp.RoutingModel(manager)

    def distance_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return data['distance_matrix'][from_node][to_node]
    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    def demand_callback(from_index):
        from_node = manager.IndexToNode(from_index)
        demand = 0
        for i, (pickup_node, delivery_node) in enumerate(data['pickups_deliveries']):
            if pickup_node == from_node: demand += tasks[i]['demand_kg']
            elif delivery_node == from_node: demand -= tasks[i]['demand_kg']
        return demand
    demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(demand_callback_index, 0, data['vehicle_capacities'], True, 'Capacity')

    for i, (pickup_node, delivery_node) in enumerate(data['pickups_deliveries']):
        pickup_index = manager.NodeToIndex(pickup_node)
        delivery_index = manager.NodeToIndex(delivery_node)
        routing.AddPickupAndDelivery(pickup_index, delivery_index)
        routing.solver().Add(routing.VehicleVar(pickup_index) == routing.VehicleVar(delivery_index))

    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = (routing_enums_pb2.FirstSolutionStrategy.PARALLEL_CHEAPEST_INSERTION)
    solution = routing.SolveWithParameters(search_parameters)

    if not solution: return []

    bids = []
    location_map_rev = {v: k for k, v in location_ids.items()}
    for vehicle_id in range(data['num_vehicles']):
        index = routing.Start(vehicle_id)
        route_nodes = []
        while not routing.IsEnd(index):
            node_index = manager.IndexToNode(index)
            if node_index != 0: route_nodes.append(node_index)
            index = solution.Value(routing.NextVar(index))
        
        if not route_nodes: continue

        stops_map = {}
        original_request_ids = set()

        # Xác định các task thuộc về lộ trình này
        tasks_in_route = [
            task for task in tasks 
            if location_ids[task['from']] in route_nodes or location_ids[task['to']] in route_nodes
        ]

        for task in tasks_in_route:
            original_request_ids.update(task['original_request_ids'])
            
            # Gom điểm PICKUP
            if task['from'] not in stops_map:
                stops_map[task['from']] = {"action": "PICKUP", "items": []}
            stops_map[task['from']]['items'].extend(task['items'])

            # Gom điểm DELIVERY
            if task['to'] not in stops_map:
                stops_map[task['to']] = {"action": "DELIVERY", "items": []}
            stops_map[task['to']]['items'].extend(task['items'])

        # Gom các item có cùng assetID bên trong mỗi điểm dừng
        for facility_id, stop_data in stops_map.items():
            final_items = []
            item_map = {}
            for item in stop_data['items']:
                asset_id = item['assetID']
                if asset_id not in item_map:
                    item_map[asset_id] = item.copy()
                    item_map[asset_id]['quantity'] = item['quantity'].copy()
                    item_map[asset_id]['quantity']['value'] = 0
                item_map[asset_id]['quantity']['value'] += item['quantity']['value']
            stop_data['items'] = list(item_map.values())

        # Chuyển đổi map thành danh sách stops theo đúng thứ tự của lộ trình
        stops = []
        for node_index in route_nodes:
            facility_id = location_map_rev[node_index]
            if facility_id in stops_map:
                stop_data = stops_map.pop(facility_id) 
                stops.append({
                    "facilityID": facility_id,
                    "action": stop_data['action'],
                    "items": stop_data['items']
                })

        vehicle_info = vehicles[vehicle_id]
        bids.append({
            "originalRequestIDs": list(original_request_ids),
            "biddingAssignments": [{"driverID": vehicle_info['ownerDriverID'], "vehicleID": vehicle_info['vehicleID']}],
            "shipmentType": "VRP_OPTIMIZED_" + vehicle_type,
            "stops": stops
        })
    return bids

# --- HÀM CHÍNH VÀ FLASK SERVER ---

app = Flask(__name__)

@app.route('/optimize', methods=['POST'])
def optimize_route():
    if not API_SERVER_URL:
        print("FATAL: API_SERVER_URL or AGENT_API_TOKEN is not set in the environment.", file=sys.stderr)
        return jsonify({"error": "Agent is not configured properly."}), 500
        
    input_data = request.get_json()
    if not input_data:
        return jsonify({"error": "Invalid JSON input"}), 400

    try:
        # BƯỚC A: TRÍCH XUẤT DỮ LIỆU TỪ INPUT
        dispatch_reqs = [req for req in input_data.get('dispatchRequests', []) if req]
        replenishment_reqs = [req for req in input_data.get('replenishmentRequests', []) if req]
        available_vehicles = input_data.get('availableVehicles', [])
        all_facilities = input_data.get('allFacilities', [])
        product_catalog_list = input_data.get('productCatalog', [])
        product_catalog = {p['sku']: p for p in product_catalog_list}

        # BƯỚC B: XỬ LÝ NGHIỆP VỤ (MATCHING)
        transport_tasks = create_transport_tasks(dispatch_reqs, replenishment_reqs, all_facilities, product_catalog)
        if not transport_tasks: return jsonify([])

        # BƯỚC C: TỐI ƯU HÓA (VRP)
        all_bids = []
        cold_chain_tasks = [t for t in transport_tasks if t['vehicle_type'] == 'COLD_CHAIN']
        cold_chain_vehicles = [v for v in available_vehicles if v['specs'].get('refrigerated') == True]
        if cold_chain_tasks and cold_chain_vehicles:
            all_bids.extend(solve_vrp_for_vehicle_type(cold_chain_tasks, cold_chain_vehicles, all_facilities, "COLD_CHAIN"))

        raw_material_tasks = [t for t in transport_tasks if t['vehicle_type'] == 'RAW_MATERIAL_TRUCK']
        raw_material_vehicles = [v for v in available_vehicles if v['specs'].get('refrigerated') == False]
        if raw_material_tasks and raw_material_vehicles:
            all_bids.extend(solve_vrp_for_vehicle_type(raw_material_tasks, raw_material_vehicles, all_facilities, "RAW_MATERIAL_TRUCK"))

        # BƯỚC D: TRẢ VỀ KẾT QUẢ
        return jsonify(all_bids)
    except Exception as e:
        print(f"An error occurred during optimization: {e}", file=sys.stderr)
        return jsonify({"error": "An internal error occurred."}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)