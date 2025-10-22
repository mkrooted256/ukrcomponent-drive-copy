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
from google.auth.exceptions import RefreshError
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import google.auth
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

import docx2pdf
import pptxtopdf

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
        except Exception as e:
            # Networking or other transient exceptions could be retried; simple backoff
            attempt += 1
            if attempt <= max_retries:
                sleep_for = min(max_delay, delay * (2 ** (attempt - 1)))
                sleep_for = sleep_for * (0.5 + random.random() * 0.5)
                logger.error(f"Unexpected error on {op_desc or 'API call'} (attempt {attempt}/{max_retries}). Sleeping {sleep_for:.1f}s: "
                             f"{e}")
                time.sleep(sleep_for)
                continue
            raise

# ----------------------------
# Drive API wrappers (with retries)
# ----------------------------
def drive_list(service, **kwargs):
    return with_retries(lambda: service.files().list(supportsAllDrives=True, **kwargs).execute(), op_desc="files.list")

def drive_get(service, file_id: str, fields: str):
    return with_retries(lambda: service.files().get(fileId=file_id, fields=fields, supportsAllDrives=True).execute(),
                        op_desc=f"files.get {file_id}")

def drive_copy(service, file_id: str, body: dict, fields: Optional[str] = None):
    return with_retries(lambda: service.files().copy(fileId=file_id, body=body, fields=fields, supportsAllDrives=True).execute(),
                        op_desc=f"files.copy {file_id}")

def drive_create(service, body: dict, fields: str):
    return with_retries(lambda: service.files().create(body=body, fields=fields, supportsAllDrives=True).execute(),
                        op_desc=f"files.create {body.get('name','<no-name>')}")

def drive_download(service, file_id: str, local_path: str):
    """Download a file from Drive to local storage"""
    # UNTESTED!!
    try:
        request = service.files().get_media(fileId=file_id)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # logger.info(f"Downloading '{file_id}'")
        with open(local_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
    except Exception as e:
        if os.path.exists(local_path):
            os.remove(local_path)
        raise

def drive_export(service, file_id: str, local_path: str, mime_type: str = 'application/pdf'):
    """Export and download a file from Drive in the specified format"""
    try:
        request = service.files().export_media(fileId=file_id, mimeType=mime_type)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        logger.info(f"Downloading '{file_id}'")
        with open(local_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            nchunk = 1
            while not done:
                if nchunk > 1: logger.debug(f"Downloading '{file_id}' chunk {nchunk}")
                status, done = downloader.next_chunk()
                nchunk += 1
    except Exception as e:
        if os.path.exists(local_path):
            os.remove(local_path)
        raise

def drive_upload(service, local_path: str, parent_id: str, desired_name: str=None, mime_type: str = None, fields: str = 'id,name,mimeType,parents'):
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Local file not found: {local_path}")
    name = desired_name or os.path.basename(local_path)
    metadata = {
        'name': name,
        'parents': [parent_id],
    }
    media = MediaFileUpload(local_path, mimetype=mime_type, resumable=True)
    def _call_create():
        req = service.files().create(
            body=metadata,
            media_body=media,
            fields=fields,
            supportsAllDrives=True  # required to work with shared drives
        )
        return req.execute()
    return with_retries(_call_create, op_desc=f"upload '{local_path}' -> '{parent_id}'")

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
                try:
                    creds.refresh(Request())
                except RefreshError:
                    logger.info("Stored credentials expired and refresh failed. Re-authenticating.")
                    creds = None
            if not creds:
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
    'status', 'dest_id', 'error', 'last_attempt', 'retries',
    'local_path'
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
def list_children(service, folder_id: str, page_token: Optional[str] = None, drive_id = None) -> Tuple[List[dict], Optional[str]]:
    if drive_id:
        # Different semantics in shared drives
        resp = drive_list(
            service,
            q=f"'{folder_id}' in parents and trashed = false",
            spaces='drive', # spaces is 'drive' or 'appDataFolder'.
            corpora='drive',
            driveId=drive_id,
            includeItemsFromAllDrives=True, # for some fucking reason
            fields='nextPageToken, files(id,name,mimeType,parents,size,modifiedTime,driveId)',
            pageToken=page_token
        )
    else:
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
        meta = drive_get(service, root_id, fields='id,name,mimeType,parents,size,modifiedTime,driveId')
        root_name = meta.get('name', root_name)
        root_driveId = meta.get('driveId', None)
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
        df.loc[item_id, ['root_id', 'root_name', 'root_dest_id', 'id', 'name', 'mimeType', 'parent_id', 'drive_id',
                         'path', 'size', 'modifiedTime', 'status', 'dest_id', 'error',
                         'last_attempt', 'retries']] = [
            root_id, root_name, root_dest_id, item['id'], item.get('name'), item.get('mimeType'),
            (item.get('parents') or [None])[0], item.get('driveId'), path, item.get('size'),
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
        'modifiedTime': meta.get('modifiedTime'),
        'driveId': root_driveId
    }
    add_row(root_item, parent_path="")

    if root_driveId:
        logger.info(f"! Root {root_name} ({root_id}) is on a shared drive (https://drive.google.com/drive/folders/{root_driveId})")

    # Iterative DFS to avoid recursion limits
    stack = [(root_id, root_name)]  # (folder_id, path)
    to_flush = 0

    while stack:
        current_id, current_path = stack.pop()
        page = None
        while True:
            children, page = list_children(service, current_id, page, drive_id=root_driveId)
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

direct_download_mime = [
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document', # docx
    'application/vnd.openxmlformats-officedocument.presentationml.presentation', # pptx
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', # xlsx
    'application/vnd.oasis.opendocument.text', # odt
    'application/vnd.oasis.opendocument.spreadsheet',
    'application/vnd.oasis.opendocument.presentation',
    'application/pdf',
    'text/plain',
    'text/markdown',
    'text/csv',
    'text/tab-separated-values',
    'image/jpeg',
    'image/png',
    'image/svg+xml',
    'image/gif'
    'image/tiff'
    'application/mp4',
    'audio/mpeg',
    'audio/ogg',
    'audio/vnd.dlna.adts' # AAC
    'application/zip'
]

GOOGLE_MIME_EXPORT_TYPES = {
    'application/vnd.google-apps.document': 'application/pdf',
    'application/vnd.google-apps.spreadsheet': 'application/pdf',
    'application/vnd.google-apps.presentation': 'application/pdf',
    'application/vnd.google-apps.drawing': 'application/pdf',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'application/pdf',
    'application/vnd.openxmlformats-officedocument.presentationml.presentation': 'application/pdf',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'application/pdf',
}

def export_inventory_as_pdf(service, df: pd.DataFrame, inventory_csv: str, output_dir: str, batch_size, token_file):
    if df.empty:
        raise RuntimeError("Inventory is empty. Run 'scan' first to build the inventory CSV.")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Index by id for in-place updates
    if 'id_indexed' not in df.attrs:
        df.set_index('id', inplace=True, drop=False)
        df.attrs['id_indexed'] = True
    
    # Filter for files only (not folders) that haven't been downloaded
    to_download = df[
        df['mimeType'].isin(GOOGLE_MIME_EXPORT_TYPES.keys()) & 
        ((df['status'] != 'pdf_downloaded') | df['status'].isna())
    ]

    nfolders = len(df[df['mimeType'] == 'application/vnd.google-apps.folder'])
    nexportable = len(df[df['mimeType'].isin(GOOGLE_MIME_EXPORT_TYPES.keys())])
    logger.info(f"Found {nfolders} folders")
    logger.info(f"Found {nfolders} folders, {nexportable} exportable files, {len(df)-nexportable-nfolders} ignored files")

    total = len(to_download)
    if total == 0:
        logger.info("No new files to download")
        return
        
    logger.info(f"Preparing to export {total} files")
    processed = 0
    
    for idx, row in to_download.iterrows():
        try:
            # Calculate local path based on drive path
            rel_path = row['path'].lstrip('/')
            local_path = os.path.join(output_dir, rel_path) + '.pdf'
            
            # Skip if already downloaded successfully
            if os.path.exists(local_path) and df.at[row['id'], 'status'] == 'pdf_downloaded':
                continue
                
            logger.info(f"Exporting to pdf: {rel_path}")
            
            # Download file
            drive_export(service, row['id'], local_path, mime_type='application/pdf')
            
            # Update status
            df.at[row['id'], 'status'] = 'pdf_downloaded'
            df.at[row['id'], 'local_path'] = local_path
            df.at[row['id'], 'error'] = None
            df.at[row['id'], 'last_attempt'] = _now_iso()
            
            processed += 1
            if processed % batch_size == 0:
                flush_inventory(df.reset_index(drop=True), inventory_csv)
                logger.info(f"Progress: {processed}/{total} files exported to pdf")
                
        except Exception as e:
            df.at[row['id'], 'status'] = 'error'
            df.at[row['id'], 'error'] = str(e)
            df.at[row['id'], 'last_attempt'] = _now_iso()
            df.at[row['id'], 'retries'] = int(df.at[row['id'], 'retries']) + 1
            flush_inventory(df.reset_index(drop=True), inventory_csv)
            logger.error(f"Error exporting to pdf {row['path']}: {e}")
    
    # Final flush
    flush_inventory(df.reset_index(drop=True), inventory_csv)
    logger.info(f"Export to pdf complete. {processed}/{total} files exported to pdf")


def download_inventory(service, df: pd.DataFrame, inventory_csv: str, output_dir: str, batch_size, token_file):
    if df.empty:
        raise RuntimeError("Inventory is empty. Run 'scan' first to build the inventory CSV.")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Index by id for in-place updates
    if 'id_indexed' not in df.attrs:
        df.set_index('id', inplace=True, drop=False)
        df.attrs['id_indexed'] = True
    
    # Filter for files only (not folders) that haven't been downloaded
    to_download = df[
        df['mimeType'].isin(GOOGLE_MIME_EXPORT_TYPES.keys()) & 
        ((df['status'] != 'downloaded') | df['status'].isna())
    ]

    nfolders = len(df[df['mimeType'] == 'application/vnd.google-apps.folder'])
    ndownloadable = len(df[df['mimeType'].isin(GOOGLE_MIME_EXPORT_TYPES.keys())])
    logger.info(f"Found {nfolders} folders")
    logger.info(f"Found {nfolders} folders, {ndownloadable} downloadable files, {len(df)-ndownloadable-nfolders} ignored files")

    total = len(to_download)
    if total == 0:
        logger.info("No new files to download")
        return
        
    logger.info(f"Preparing to download {total} files")
    processed = 0
    
    for idx, row in to_download.iterrows():
        try:
            # Calculate local path based on drive path
            rel_path = row['path'].lstrip('/')
            local_path = os.path.join(output_dir, rel_path)
            pdf_path = os.path.join(output_dir, rel_path) + '.pdf'
            
            # Skip if already downloaded successfully
            if os.path.exists(local_path) and df.at[row['id'], 'status'] == 'downloaded':
                continue
                
            logger.info(f"Downloading: {rel_path}")
            
            # Download file
            drive_download(service, row['id'], local_path)
            
            # Update status
            df.at[row['id'], 'status'] = 'downloaded'
            df.at[row['id'], 'local_path'] = local_path
            df.at[row['id'], 'error'] = None
            df.at[row['id'], 'last_attempt'] = _now_iso()
            
            processed += 1
            if processed % batch_size == 0:
                flush_inventory(df.reset_index(drop=True), inventory_csv)
                logger.info(f"Progress: {processed}/{total} files downloaded")
                
        except Exception as e:
            df.at[row['id'], 'status'] = 'error'
            df.at[row['id'], 'error'] = str(e)
            df.at[row['id'], 'last_attempt'] = _now_iso()
            df.at[row['id'], 'retries'] = int(df.at[row['id'], 'retries']) + 1
            flush_inventory(df.reset_index(drop=True), inventory_csv)
            logger.error(f"Error downloading {row['path']}: {e}")
    
    # Final flush
    flush_inventory(df.reset_index(drop=True), inventory_csv)
    logger.info(f"Download complete. {processed}/{total} files downloaded")

def convert_inventory_to_pdf(service, df_input: pd.DataFrame, inventory_csv: str, local_parent_dir: str, selected_roots: List[str], token_file, name_prefix: str = ""):
    processed = 0

    # Filter out unconvertable 
    df = df_input[ (df_input['mimeType'] == 'application/vnd.google-apps.folder') | (df_input['mimeType'].isin(GOOGLE_MIME_EXPORT_TYPES.keys())) ]

    # Index by id for in-place updates
    if 'id_indexed' not in df.attrs:
        df.set_index('id', inplace=True, drop=False)
        df.attrs['id_indexed'] = True
    
    nfolders = len(df[df['mimeType'] == 'application/vnd.google-apps.folder'])
    nexportable = len(df) - nfolders

    logger.info(f"Found {nfolders} folders, {nexportable} exportable files, {len(df_input)-nexportable-nfolders} ignored files")

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
    logger.info(f"Starting downloading, converting, and uploading of {total} items under {len(selected_roots)} root(s).")

    nprocessed_files = 1
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
                logger.info(f"[{nprocessed_files}/{nexportable}] Processing '{row['path']}' ({src_id})")
                
                if not (row['mimeType'] in GOOGLE_MIME_EXPORT_TYPES.keys()):
                    raise ValueError(f"Invalid mime type '{row['mimeType']}' for '{src_id}'")
                
                # 1. Download
                rel_path = row['path'].lstrip('/')
                local_path = os.path.join(local_parent_dir, rel_path)
                local_path_dir = os.path.dirname(local_path)
                filename,ext = os.path.splitext(os.path.basename(local_path))
                pdf_path = os.path.join(local_path_dir, filename + '.pdf')

                downloaded = False
                if os.path.exists(local_path):
                    logger.info(f"> File '{src_id}' is already downloaded. Skipping download.")
                else:
                    downloaded = True
                    logger.info(f"> Downloading {src_id}")
                    drive_download(service, src_id, local_path)

                # 2. Convert
                converted = False
                if downloaded or not os.path.exists(pdf_path):
                    logger.info(f"> Converting {src_id}")
                    if local_path.endswith('.docx'):
                        docx2pdf.convert(local_path, pdf_path)
                    elif local_path.endswith('.pptx'):
                        pptxtopdf.convert(local_path, local_path_dir)
                    else:
                        raise ValueError(f"Invalid extention '{ext}' when converting to pdf. Only .docx and .pptx are supported.")
                    converted = True
                else:
                    logger.info(f"> PDF '{pdf_path}' already exists. Skipping convert.")

                # 3. Upload
                # We cannot check if already copied, so upload always
                desired_name = row['name'] + '.pdf'
                logger.info(f"> Uploading {src_id}")
                uploaded = drive_upload(service, pdf_path, dest_parent_id, desired_name=desired_name)

                if not uploaded.get('id', None):
                    logger.error(f"Error uploading '{src_id}' -> {pdf_path} -> {dest_parent_id}:"
                                 f"{uploaded}")
                    raise ValueError(f"Error uploading '{src_id}' -> {pdf_path} -> {dest_parent_id}")

                # 4. Finalize
                df.at[src_id, 'dest_id'] = uploaded['id']
                df.at[src_id, 'status'] = 'done'
                df.at[src_id, 'error'] = None
                nprocessed_files += 1

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
            logger.error(f"Error processing {row['path']}: {e}")
        except Exception as e:
            df.at[src_id, 'status'] = 'error'
            df.at[src_id, 'error'] = str(e)
            df.at[src_id, 'retries'] = int(df.at[src_id, 'retries']) + 1
            flush_inventory(df.reset_index(drop=True), inventory_csv)
            logger.error(f"Error processing {row['path']}: {e}")

    logger.info("Upload converted PDF complete.")

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
                r['destination_id'] = default_dest_id
            if 'id' not in r or 'name' not in r:
                raise ValueError("folders.csv must have columns: name,id")
            if r['id'].startswith('https://') or r['id'].startswith('drive.google'):
                r['id'] = gdrive_url_to_id(r['id'])
            if (r['destination_id'] and 
                (r['destination_id'].startswith('https://') or r['destination_id'].startswith('drive.google'))):
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

import argparse
import sys
from collections import namedtuple

ScanParams = namedtuple('ScanParams','folders_csv,inventory_csv,dest_id,batch_flush,token_file')
CopyParams = namedtuple('CopyParams','folders_csv,inventory_csv,dest_id,select_root_ids,name_prefix,token_file')
DownloadParams = namedtuple('DownloadParams', 'inventory_csv,output_dir,batch_size,token_file')

def cmd_scan(args: ScanParams):
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

def cmd_copy(args: CopyParams):
    service = auth_drive(args.token_file)
    folders = read_folders_csv(args.folders_csv, default_dest_id=args.dest_id)
    selected_root_ids = parse_root_selection(folders, args.select_root_ids)

    df = load_inventory(args.inventory_csv)
    if df.empty:
        raise RuntimeError("Inventory is empty. Run 'scan' first to build the inventory CSV.")

    logger.info(f"Preparing to copy {len(selected_root_ids)} root(s)")
    perform_copy(service, df, args.inventory_csv, selected_root_ids, name_prefix=args.name_prefix or "")

def cmd_download(args: DownloadParams):
    pass

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
