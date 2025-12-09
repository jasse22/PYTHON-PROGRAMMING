import base64
import json
import os
import sys
import time
import argparse
from typing import Dict, List, Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


API_BASE = "https://open-reaction-database.org/api"
USER_AGENT = "ORD-Scraper-Advanced/3.0"
CORE_CATEGORIES = {"base", "solvent", "amine", "aryl halide", "metal", "ligand"}
REACTION_TIMEOUT_S = 90



def make_session() -> requests.Session:
    """Creates a requests session with retry logic for robust API calls."""
    session = requests.Session()
    retries = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.0,  
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    })
    return session

def submit_query(session: requests.Session, dataset_id: str, limit: int) -> str:
    """Submits a query to fetch reactions and returns the task ID."""
    url = f"{API_BASE}/submit_query"
    params = {"dataset_id": dataset_id, "limit": limit}
    resp = session.get(url, params=params, timeout=30)
    resp.raise_for_status()
    
    return resp.text.strip().strip('"')

def fetch_query_result(session: requests.Session, task_id: str) -> List[Dict]:
    """Polls for query results until ready or timeout."""
    url = f"{API_BASE}/fetch_query_result"
    params = {"task_id": task_id}
    start_time = time.time()
    
    while time.time() - start_time < REACTION_TIMEOUT_S:
        resp = session.get(url, params=params, timeout=30)
        
        if resp.status_code == 200:
            try:
                return resp.json()
            except json.JSONDecodeError:
                return []
        
        
        elif resp.status_code == 202:
            time.sleep(2.0)
            continue
        
        
        elif resp.status_code == 400 and "not ready" in resp.text.lower():
            time.sleep(2.0)
            continue
        
        resp.raise_for_status() 

    raise TimeoutError(f"Query result for task={task_id} timed out after {REACTION_TIMEOUT_S}s.")



def decode_reaction_proto(proto_b64: str):
    """Decodes base64-encoded reaction Protocol Buffer data using ord_schema."""
    try:
        from ord_schema.proto import reaction_pb2
    except ImportError:
        print("ERROR: ord-schema is not installed. Run 'pip install ord-schema requests'.", file=sys.stderr)
        sys.exit(1)
        
    raw = base64.b64decode(proto_b64)
    rxn = reaction_pb2.Reaction()
    rxn.ParseFromString(raw)
    return rxn

def extract_identifiers(compound) -> str:
    """Extracts and joins all non-empty compound identifier values."""
    texts = []
    for ident in compound.identifiers:
        if ident.value:
            texts.append(ident.value)
    return "; ".join(texts)

def extract_reaction_data(rxn, dataset_id: str) -> Dict[str, Any]:
    """
    Parses a decoded Reaction protobuf object to extract structured data.
    """
    extracted_roles: Dict[str, List[Dict]] = {k: [] for k in CORE_CATEGORIES}
    
    try:
        
        outcome_successful = False
        if rxn.outcomes and rxn.outcomes[0].reaction_product:
            outcome_successful = True 

        
        for input_key, reaction_input in rxn.inputs.items():
            normalized_key = input_key.strip().lower().replace("_", " ")

            for component in reaction_input.components:
                text_id = extract_identifiers(component)
                
                if not text_id:
                    continue
                
                
                try:
                    from ord_schema.proto import reaction_pb2
                    role_name = reaction_pb2.ReactionRole.Name(component.reaction_role)
                except Exception:
                    role_name = "UNKNOWN"

                comp_data = {
                    "value": text_id,
                    "role": role_name
                }
                
                
                assigned = False
                for core_key in CORE_CATEGORIES:
                    if core_key in normalized_key:
                        extracted_roles[core_key].append(comp_data)
                        assigned = True
                        break
                
                
                if not assigned:
                    extracted_roles.setdefault(normalized_key, []).append(comp_data)
                    
        return {
            "dataset_id": dataset_id,
            "reaction_id": rxn.reaction_id.value,
            "components": extracted_roles,
            "success": outcome_successful
        }
    
    except Exception as e:
        print(f"Warning: Failed to extract data for reaction {rxn.reaction_id.value}: {e}", file=sys.stderr)
        return {
            "dataset_id": dataset_id,
            "reaction_id": rxn.reaction_id.value,
            "error": str(e)
        }



def scrape_ord_advanced(
    max_datasets: Optional[int],
    per_dataset_limit: int,
    dataset_ids: Optional[List[str]]
) -> List[Dict]:
    """Coordinates the entire scraping process."""
    session = make_session()
    all_results: List[Dict] = []
    processed_count = 0
    
    print("Fetching list of all datasets...", file=sys.stderr)
    datasets = session.get(f"{API_BASE}/datasets", timeout=30).json()
    
    if dataset_ids:
        
        datasets = [d for d in datasets if d.get("dataset_id") in dataset_ids]
    
    print(f"Found {len(datasets)} datasets matching criteria.", file=sys.stderr)

    for ds in datasets:
        dataset_id = ds.get("dataset_id")
        num_rxns = ds.get("num_reactions", 0)
        
        if max_datasets is not None and processed_count >= max_datasets:
            break
            
        print(f"\n[{processed_count + 1}] Processing: {dataset_id} ({num_rxns} total reactions)", file=sys.stderr)
        
        try:
            effective_limit = per_dataset_limit if per_dataset_limit > 0 else num_rxns
            
            task_id = submit_query(session, dataset_id, limit=effective_limit)
            items = fetch_query_result(session, task_id)
            
            print(f"  -> Retrieved {len(items)} reactions for parsing.", file=sys.stderr)
            
            for item in items:
                proto_b64 = item.get("proto")
                if not proto_b64:
                    continue
                
                rxn = decode_reaction_proto(proto_b64)
                data = extract_reaction_data(rxn, dataset_id)
                all_results.append(data)
                
            processed_count += 1
            
        except Exception as e:
            print(f"  -> ERROR during dataset {dataset_id} processing: {e}", file=sys.stderr)
            all_results.append({"dataset_id": dataset_id, "error": str(e)})
            
        
        time.sleep(1.0) 
        
    return all_results



def main():
    parser = argparse.ArgumentParser(
        description="Advanced scraper for the Open Reaction Database (ORD) API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape 3 datasets with 10 reactions each
  python ord_advanced_scraper.py --max_datasets 3 --limit 10
  
  # Scrape a specific dataset (all reactions)
  python ord_advanced_scraper.py --dataset_ids ord_dataset-3b7692e9d29b43179261358b13997fef --limit 0
        """
    )
    
    parser.add_argument(
        "--max_datasets",
        type=int,
        default=2,
        help="Maximum number of datasets to scrape (default: 2). Use 0 for ALL."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        dest="per_dataset_limit",
        help="Number of reactions per dataset (default: 5). Use 0 for ALL reactions in a dataset."
    )
    parser.add_argument(
        "--dataset_ids",
        type=str,
        default=None,
        help="Comma-separated list of specific dataset IDs to scrape."
    )
    parser.add_argument(
        "--json_out",
        default=os.path.join(os.getcwd(), "ord_scrape_results.json"),
        help="Output JSON file path (default: ord_scrape_results.json)"
    )
    
    args = parser.parse_args()
    
    
    ds_ids: Optional[List[str]] = None
    if args.dataset_ids:
        ds_ids = [x.strip() for x in args.dataset_ids.split(",") if x.strip()]
        
    if args.max_datasets == 0 and not ds_ids:
        print("\n!!! WARNING: You requested ALL datasets. This will take a long time. !!!", file=sys.stderr)
        args.max_datasets = None 

    print("\n" + "="*50, file=sys.stderr)
    print("🤖 Starting Advanced ORD Scraper", file=sys.stderr)
    print(f"Datasets to process: {args.max_datasets or 'ALL'}", file=sys.stderr)
    print(f"Reactions/Dataset: {args.per_dataset_limit or 'ALL'}", file=sys.stderr)
    print(f"Output File: {args.json_out}", file=sys.stderr)
    print("="*50 + "\n", file=sys.stderr)
    
    try:
        rows = scrape_ord_advanced(
            max_datasets=args.max_datasets,
            per_dataset_limit=args.per_dataset_limit,
            dataset_ids=ds_ids
        )
        
        
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
            
        print("\n" + "="*50, file=sys.stderr)
        print(f" Scrape Complete! Total reactions processed: {len(rows)}", file=sys.stderr)
        print(f"File saved to: {args.json_out}", file=sys.stderr)
        print("="*50 + "\n", file=sys.stderr)
        
    except Exception as e:
        print(f"\n CRITICAL FAILURE: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()