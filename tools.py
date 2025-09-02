"""
MIT License

Copyright (c) 2025 Mykhailo Koreshkov, Learn and Teach UA NGO
Using Microsoft Copilot

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""


import csv
from collections import namedtuple
import os
import re
import pandas as pd
import logging
logger = logging.getLogger("ukrcomp-u-tools")
import pickle
import json
import random
import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import google.auth
from googleapiclient.http import MediaFileUpload

SCOPES = [
  "https://www.googleapis.com/auth/drive.metadata.readonly",
  "https://www.googleapis.com/auth/drive"
  ]


__gdrive_url_patterns = [
    re.compile(r'/folders/([^/?&]+)'),
    re.compile(r'open\?id=([^/?&]+)'),
    re.compile(r'/d/([^/?&]+)'),
]
def gdrive_url_to_id(url: str) -> str:
    for p in __gdrive_url_patterns:
        s = p.search(url)
        if s and s[1]: return s[1]
    return None


def _now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat() + 'Z'


# ----------------------------
# Retry helpers
# ----------------------------
RETRYABLE_STATUSES = {429, 500, 503}
RETRYABLE_403_REASONS = {'rateLimitExceeded', 'userRateLimitExceeded', 'backendError'}
from googleapiclient.errors import HttpError

def is_retryable_http_error(e: HttpError) -> bool:
    try:
        status = e.resp.status
    except Exception:
        return False
    if status in RETRYABLE_STATUSES:
        return True
    if status == 403:
        try:
            payload = json.loads(e.content.decode('utf-8'))
            errs = payload.get('error', {}).get('errors', [])
            reasons = {it.get('reason') for it in errs if isinstance(it, dict)}
            if reasons & RETRYABLE_403_REASONS:
                return True
        except Exception:
            pass
    return False

def with_retries(call, max_retries=8, initial_delay=1.0, max_delay=64.0, op_desc: str = ""):
    attempt = 0
    delay = initial_delay
    while True:
        try:
            return call()
        except HttpError as e:
            retryable = is_retryable_http_error(e)
            attempt += 1
            if retryable and attempt <= max_retries:
                sleep_for = min(max_delay, delay * (2 ** (attempt - 1)))
                # Full jitter
                sleep_for = sleep_for * (0.5 + random.random() * 0.5)
                logger.info(f"Retryable API error on {op_desc or 'API call'} (attempt {attempt}/{max_retries}): "
                     f"HTTP {getattr(e.resp, 'status', '?')}. Sleeping {sleep_for:.1f}s")
                time.sleep(sleep_for)
                continue
            # Non-retryable or exhausted retries
            raise
        except Exception:
            # Networking or other transient exceptions could be retried; simple backoff
            attempt += 1
            if attempt <= max_retries:
                sleep_for = min(max_delay, delay * (2 ** (attempt - 1)))
                sleep_for = sleep_for * (0.5 + random.random() * 0.5)
                logger.error(f"Unexpected error on {op_desc or 'API call'} (attempt {attempt}/{max_retries}). Sleeping {sleep_for:.1f}s")
                time.sleep(sleep_for)
                continue
            raise

# ----------------------------
# Drive API wrappers (with retries)
# ----------------------------
def drive_list(service, **kwargs):
    return with_retries(lambda: service.files().list(supportsAllDrives=True,**kwargs).execute(), op_desc="files.list")

def drive_get(service, file_id: str, fields: str):
    return with_retries(lambda: service.files().get(fileId=file_id, fields=fields, supportsAllDrives=True).execute(),
                        op_desc=f"files.get {file_id}")

def drive_copy(service, file_id: str, body: dict, fields: Optional[str] = None):
    return with_retries(lambda: service.files().copy(fileId=file_id, body=body, fields=fields, supportsAllDrives=True).execute(),
                        op_desc=f"files.copy {file_id}")

def drive_create(service, body: dict, fields: str):
    return with_retries(lambda: service.files().create(body=body, fields=fields, supportsAllDrives=True).execute(),
                        op_desc=f"files.create {body.get('name','<no-name>')}")

def auth_drive(token_file):
    creds = None
    if token_file:
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                logger.info("Token ok but expired. Refreshing")
                creds.refresh(Request())
                # with open(token_file, "w") as token:
                #     token.write(creds.to_json())
            else:
                raise ValueError("Invalid token file")
    else:
        if os.path.exists("token.json"):
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    "credentials.json", SCOPES
                )
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open("token.json", "w") as token:
                token.write(creds.to_json())
    
    service = build("drive", "v3", credentials=creds)
    return service

# ----------------------------
# Inventory persistence
# ----------------------------
INV_COLUMNS = [
    'root_id', 'root_name',
    'id', 'name', 'mimeType', 'parent_id', 'path',
    'size', 'modifiedTime',
    'status', 'dest_id', 'error', 'last_attempt', 'retries'
]

def ensure_inventory(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df is None:
        df = pd.DataFrame(columns=INV_COLUMNS)
    for col in INV_COLUMNS:
        if col not in df.columns:
            df[col] = None
    # Normalize types
    if 'retries' in df.columns:
        df['retries'] = pd.to_numeric(df['retries'], errors='coerce').fillna(0).astype(int)
    if 'status' in df.columns:
        df['status'] = df['status'].fillna('pending')
    return df

def load_inventory(inventory_csv: str) -> pd.DataFrame:
    if os.path.exists(inventory_csv):
        df = pd.read_csv(inventory_csv, dtype=str, keep_default_na=False, na_values=[])
        df = ensure_inventory(df)
        # Normalize columns
        df['retries'] = pd.to_numeric(df['retries'], errors='coerce').fillna(0).astype(int)
        return df
    return ensure_inventory(None)

def flush_inventory(df: pd.DataFrame, inventory_csv: str):
    tmp = inventory_csv + '.tmp'
    # Write with a stable column order
    out_cols = INV_COLUMNS
    missing = [c for c in df.columns if c not in out_cols]
    out_cols = out_cols + missing
    df[out_cols].to_csv(tmp, index=False)
    os.replace(tmp, inventory_csv)


# ----------------------------
# Scan logic
# ----------------------------
def list_children(service, folder_id: str, page_token: Optional[str] = None) -> Tuple[List[dict], Optional[str]]:
    resp = drive_list(
        service,
        q=f"'{folder_id}' in parents and trashed = false",
        spaces='drive',
        fields='nextPageToken, files(id,name,mimeType,parents,size,modifiedTime)',
        pageToken=page_token
    )
    return resp.get('files', []), resp.get('nextPageToken')

def scan_root(service, df: pd.DataFrame, root_id: str, root_name: str, root_dest_id:str, inventory_csv: str, batch_flush: int = 200):
    # Get root metadata (name may be refreshed)
    try:
        meta = drive_get(service, root_id, fields='id,name,mimeType,parents,size,modifiedTime')
        root_name = meta.get('name', root_name)
    except HttpError as e:
        logger.error(f"Failed to read root {root_id}: {e}")
        return

    # DataFrame index by id for fast checks
    if 'id_indexed' not in df.attrs:
        df.set_index('id', inplace=True, drop=False)
        df.attrs['id_indexed'] = True

    def add_row(item: dict, parent_path: str):
        item_id = item['id']
        path = f"{parent_path}/{item['name']}" if parent_path else item['name']
        if item_id in df.index:
            # Already present: update path if missing; keep previous status
            if not df.at[item_id, 'path']:
                df.at[item_id, 'path'] = path
            return False
        df.loc[item_id, ['root_id', 'root_name', 'root_dest_id', 'id', 'name', 'mimeType', 'parent_id',
                         'path', 'size', 'modifiedTime', 'status', 'dest_id', 'error',
                         'last_attempt', 'retries']] = [
            root_id, root_name, root_dest_id, item['id'], item.get('name'), item.get('mimeType'),
            (item.get('parents') or [None])[0], path, item.get('size'),
            item.get('modifiedTime'), 'pending', None, None, None, 0
        ]
        return True

    # Ensure root row exists
    root_item = {
        'id': root_id,
        'name': root_name,
        'root_dest_id': root_dest_id,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [None],
        'size': None,
        'modifiedTime': meta.get('modifiedTime')
    }
    add_row(root_item, parent_path="")

    # Iterative DFS to avoid recursion limits
    stack = [(root_id, root_name)]  # (folder_id, path)
    to_flush = 0

    while stack:
        current_id, current_path = stack.pop()
        page = None
        while True:
            children, page = list_children(service, current_id, page)
            for ch in children:
                is_new = add_row(ch, parent_path=current_path)
                if ch['mimeType'] == 'application/vnd.google-apps.folder':
                    stack.append((ch['id'], f"{current_path}/{ch['name']}"))
                if is_new:
                    to_flush += 1
                    if to_flush >= batch_flush:
                        flush_inventory(df.reset_index(drop=True), inventory_csv)
                        to_flush = 0
            if not page:
                break

    if to_flush > 0:
        flush_inventory(df.reset_index(drop=True), inventory_csv)

# ----------------------------
# Copy logic
# ----------------------------
def ensure_dest_folder(service, name: str, parent_id: str) -> str:
    body = {
        'name': name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_id]
    }
    created = drive_create(service, body, fields='id')
    return created['id']

def copy_one_file(service, src_file_id, dest_parent_id, desired_name=None):
    if desired_name is None:
        body = { 'parents': [dest_parent_id] }
    else:
        body = { 'parents': [dest_parent_id], 'name': desired_name }
    created = drive_copy(service, src_file_id, body=body, fields='id')
    return created['id']

def validate_dest_is_folder(service, dest_id: str):
    meta = drive_get(service, dest_id, fields='id,name,mimeType')
    if meta.get('mimeType') != 'application/vnd.google-apps.folder':
        raise ValueError(f"Destination ID {dest_id} is not a folder.")

def plan_items_for_copy(df: pd.DataFrame, selected_roots: List[str]) -> pd.DataFrame:
    # Filter inventory rows by selected root_id
    subset = df[df['root_id'].isin(selected_roots)].copy()
    # Sort: folders first, then by path depth ascending, then files by path depth ascending
    def depth(p):
        return 0 if not p else len(str(p).split('/'))
    subset['is_folder'] = (subset['mimeType'] == 'application/vnd.google-apps.folder')
    subset['depth'] = subset['path'].apply(depth)
    subset.sort_values(by=['is_folder', 'depth', 'path'], ascending=[False, True, True], inplace=True)
    return subset

def perform_copy(service,
                 df: pd.DataFrame,
                 inventory_csv: str,
                 selected_roots: List[str],
                #  dest_root_parent_id: str,
                 name_prefix: str = ""):
    # validate_dest_is_folder(service, dest_root_parent_id)

    # Index by id for in-place updates
    if 'id_indexed' not in df.attrs:
        df.set_index('id', inplace=True, drop=False)
        df.attrs['id_indexed'] = True

    processed = 0

    # For each selected root, ensure the top-level destination folder exists or create it
    folder_map: Dict[str, str] = {}  # src_folder_id -> dest_folder_id
    for root_id in selected_roots:
        if root_id not in df.index:
            logger.info(f"Root {root_id} not found in inventory; skip.")
            continue
        root_row = df.loc[root_id]
        if root_row['mimeType'] != 'application/vnd.google-apps.folder':
            logger.info(f"Root {root_id} is not a folder; skip.")
            continue
        dest_id = root_row.get('dest_id')
        if dest_id and str(root_row.get('status', '')) == 'done':
            folder_map[root_id] = dest_id
            logger.info(f"Root already copied: {root_row['name']} -> {dest_id}")
            continue
        # Create destination root folder (or reuse if dest_id already exists)
        try:
            if dest_id:
                # Validate it still exists and is a folder
                logger.info(f"Verifying root destination {root_row['name']} -> {dest_id}")
                meta = drive_get(service, dest_id, fields='id,mimeType')
                if meta.get('mimeType') != 'application/vnd.google-apps.folder':
                    raise ValueError("Existing dest_id is not a folder.")
                new_dest_id = dest_id
            else:
                dest_name = f"{name_prefix}{root_row['name']}" if name_prefix else root_row['name']
                new_dest_id = ensure_dest_folder(service, dest_name, root_row['root_dest_id'])
            folder_map[root_id] = new_dest_id
            # Update row
            df.at[root_id, 'dest_id'] = new_dest_id
            df.at[root_id, 'status'] = 'done'
            df.at[root_id, 'error'] = None
            df.at[root_id, 'last_attempt'] = _now_iso()
            flush_inventory(df.reset_index(drop=True), inventory_csv)
            processed += 1
            logger.info(f"Prepared root: {root_row['name']} -> {new_dest_id}")
        except Exception as e:
            df.at[root_id, 'status'] = 'error'
            df.at[root_id, 'error'] = str(e)
            df.at[root_id, 'last_attempt'] = _now_iso()
            df.at[root_id, 'retries'] = int(df.at[root_id, 'retries']) + 1
            flush_inventory(df.reset_index(drop=True), inventory_csv)
            logger.error(f"Failed to prepare root {root_row['name']}: {e}")

    # Prepare copy plan: folders first (shallow to deep), then files
    plan = plan_items_for_copy(df.reset_index(drop=True), selected_roots)

    # Build quick lookup from src_id -> parent_id for ordering validation
    parent_of: Dict[str, Optional[str]] = {row['id']: (row['parent_id'] if row['parent_id'] else None)
                                           for _, row in plan.iterrows()}

    total = len(plan)
    logger.info(f"Starting copy of {total} items under {len(selected_roots)} root(s).")

    for _, row in plan.iterrows():
        src_id = row['id']
        if df.at[src_id, 'status'] == 'done' and df.at[src_id, 'dest_id']:
            continue  # Already copied

        is_folder = (row['mimeType'] == 'application/vnd.google-apps.folder')
        src_parent = parent_of.get(src_id)

        # Determine destination parent folder ID
        if src_parent is None:
            # Root item itself
            dest_parent_id = row['root_dest_id']
        else:
            # For folders/files not at root, the parent must have been created already
            parent_dest = df.at[src_parent, 'dest_id'] if (src_parent in df.index and df.at[src_parent, 'dest_id']) else None
            if not parent_dest:
                # If parent not yet created, skip for now (shouldn't happen due to sorting), or retry next run
                logger.error(f"Parent destination missing for {row['path']} (parent {src_parent}). Skipping this run.")
                continue
            dest_parent_id = parent_dest

        try:
            df.at[src_id, 'last_attempt'] = _now_iso()
            if is_folder:
                # Create folder if not created
                if df.at[src_id, 'dest_id']:
                    # Validate it's a folder
                    meta = drive_get(service, df.at[src_id, 'dest_id'], fields='id,mimeType')
                    if meta.get('mimeType') != 'application/vnd.google-apps.folder':
                        raise ValueError("Existing dest_id is not a folder.")
                    dest_id = df.at[src_id, 'dest_id']
                else:
                    dest_id = ensure_dest_folder(service, row['name'], dest_parent_id)
                df.at[src_id, 'dest_id'] = dest_id
                df.at[src_id, 'status'] = 'done'
                df.at[src_id, 'error'] = None
            else:
                # Copy file
                new_id = copy_one_file(service, src_id, dest_parent_id, desired_name=row['name'])
                df.at[src_id, 'dest_id'] = new_id
                df.at[src_id, 'status'] = 'done'
                df.at[src_id, 'error'] = None

            flush_inventory(df.reset_index(drop=True), inventory_csv)
            processed += 1
            # if processed % 50 == 0:
            #     logger.info(f"Progress: {processed}/{total} items done.")
            logger.info(f"... '{df.at[src_id, 'path']}' done [{processed}/{total}]")
        except HttpError as e:
            df.at[src_id, 'status'] = 'error'
            df.at[src_id, 'error'] = f"HTTP {getattr(e.resp, 'status', '?')}: {e}"
            df.at[src_id, 'retries'] = int(df.at[src_id, 'retries']) + 1
            flush_inventory(df.reset_index(drop=True), inventory_csv)
            logger.error(f"Error copying {row['path']}: {e}")
        except Exception as e:
            df.at[src_id, 'status'] = 'error'
            df.at[src_id, 'error'] = str(e)
            df.at[src_id, 'retries'] = int(df.at[src_id, 'retries']) + 1
            flush_inventory(df.reset_index(drop=True), inventory_csv)
            logger.error(f"Error copying {row['path']}: {e}")

    logger.info("Copy complete.")


# ----------------------------
# CLI
# ----------------------------
def read_folders_csv(path: str, default_dest_id=None) -> List[dict]:
    rows: List[dict] = []
    if not os.path.exists(path):
        raise FileNotFoundError(f"folders csv not found: {path}")
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i,r in enumerate(reader):
            if not r.get('destination_id',None):
                if default_dest_id: 
                    r['destination_id'] = default_dest_id
                else: 
                    raise ValueError(f"Either folders.csv must have 'destination_id' column or dest_id argument must be defined on row {i}")
            if 'id' not in r or 'name' not in r:
                raise ValueError("folders.csv must have columns: name,id")
            if r['id'].startswith('https://') or r['id'].startswith('drive.google'):
                r['id'] = gdrive_url_to_id(r['id'])
            if r['destination_id'].startswith('https://') or r['destination_id'].startswith('drive.google'):
                r['destination_id'] = gdrive_url_to_id(r['destination_id'])
            rows.append({'id': r['id'], 'name': r['name'], 'destination_id': r['destination_id'], 'selected': str(r.get('selected', '')).strip().lower()})
    return rows

def parse_root_selection(folder_rows: List[dict], explicit_ids: Optional[str]) -> List[str]:
    if explicit_ids:
        ids = [s.strip() for s in explicit_ids.split(',') if s.strip()]
        return ids
    # fallback to selected column
    ids = [r['id'] for r in folder_rows if r.get('selected') in ('true', '1', 'yes', 'y')]
    if ids:
        return ids
    # If none specified, default to all
    return [r['id'] for r in folder_rows]

def cmd_scan(args):
    service = auth_drive(args.token_file)
    df = load_inventory(args.inventory_csv)
    folders = read_folders_csv(args.folders_csv, default_dest_id=args.dest_id)

    logger.info(f"Scanning {len(folders)} root folder(s)...")
    for r in folders:
        logger.info(f"Scanning root: {r['name']} ({r['id']})")
        scan_root(service, df, r['id'], r['name'], r['destination_id'], args.inventory_csv, batch_flush=args.batch_flush)
    # Final flush normalize
    flush_inventory(df.reset_index(drop=True), args.inventory_csv)
    logger.info(f"Scan complete. Inventory saved to {args.inventory_csv}")

def cmd_copy(args):
    service = auth_drive(args.token_file)
    folders = read_folders_csv(args.folders_csv, default_dest_id=args.dest_id)
    selected_root_ids = parse_root_selection(folders, args.select_root_ids)

    df = load_inventory(args.inventory_csv)
    if df.empty:
        raise RuntimeError("Inventory is empty. Run 'scan' first to build the inventory CSV.")

    logger.info(f"Preparing to copy {len(selected_root_ids)} root(s)")
    perform_copy(service, df, args.inventory_csv, selected_root_ids, name_prefix=args.name_prefix or "")

import argparse
import sys
def main():
    parser = argparse.ArgumentParser(description="Google Drive scan and selective copy with progress tracking.")
    sub = parser.add_subparsers(dest='command', required=True)

    p_scan = sub.add_parser('scan', help='Scan root folders and build/update inventory CSV')
    p_scan.add_argument('--folders-csv', default='folders.csv', help='Path to roots list CSV (name,id[,selected])')
    p_scan.add_argument('--inventory-csv', default='drive_inventory.csv', help='Path to inventory CSV to write/update')
    p_copy.add_argument('--dest-id', default=None, help='Default destination parent folder ID (for folders without destination_id)')
    p_scan.add_argument('--batch-flush', type=int, default=200, help='Flush inventory to disk after this many new rows')
    p_scan.add_argument('--token-file', default=None, help='Custom google drive api token')
    p_scan.set_defaults(func=cmd_scan)

    p_copy = sub.add_parser('copy', help='Copy selected roots to destination, updating inventory CSV')
    p_copy.add_argument('--folders-csv', default='folders.csv', help='Path to roots list CSV (name,id[,selected])')
    p_copy.add_argument('--inventory-csv', default='drive_inventory.csv', help='Path to inventory CSV to read/update')
    p_copy.add_argument('--dest-id', default=None, help='Default destination parent folder ID (for folders without destination_id)')
    p_copy.add_argument('--select-root-ids', default=None, help='Comma-separated list of root IDs to copy (overrides selected column)')
    p_copy.add_argument('--name-prefix', default='', help='Prefix for destination root folder names')
    p_scan.add_argument('--token-file', default=None, help='Custom google drive api token')
    p_copy.set_defaults(func=cmd_copy)

    args = parser.parse_args()
    try:
        args.func(args)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
