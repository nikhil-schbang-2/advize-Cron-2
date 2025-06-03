import asyncio
import hashlib
import json
import random
import os
import traceback
from datetime import date, datetime, timedelta
import logging
import uuid
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError
import requests
from sqlalchemy import bindparam, text
from logging.handlers import TimedRotatingFileHandler
from facebook_business.adobjects.ad import Ad
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
import time
from itertools import islice
from typing import List, Dict, Any, Set
import psycopg2 as psy

from app.config import META_API_KEY, S3_BUCKET_NAME
from app.video_fetcher import get_carousel_images_from_facebook_iframe
from database.db import Database
from app.fact_table import upsert_dimension
from app.tasks import (
    determine_creative_type,
    download_and_upload_image,
    fetch_image_urls_batch,
    process_carousel_creative,
    process_image_creative,
    process_video_creative,
    process_ad,
    update_carousel_ad_availability,
)
from app.celery_app import celery
from app.facebook_sdk import FacebookAPIClient
from facebook_business.exceptions import FacebookRequestError
from app.utils import log_message

def exponential_backoff_retry(func, max_retries=5):
    """Retry a function with exponential backoff on rate limit or concurrency errors."""
    for attempt in range(max_retries):
        try:
            return func()
        except FacebookRequestError as e:
            error_code = e.api_error_code()
            error_subcode = e.api_error_subcode()
            error_msg = e.api_error_message()

            log_message(f"[Attempt {attempt + 1}] Facebook API Error: {error_msg}")

            if error_code in [17, 613, 80004] or error_subcode in [1487390, 80004]:
                wait_time = (2**attempt) + random.uniform(0, 1)
                log_message(
                    f"Rate/concurrency limit hit. Retrying in {wait_time:.2f}s..."
                )
                time.sleep(wait_time + (60 * attempt))
            else:
                raise
    raise Exception("Maximum retries exceeded")


def start_async_insights_query(ad_account_id: str, params: Dict[str, Any]):
    """Start an asynchronous insights query for the given ad account."""

    def _start():
        ad_account = AdAccount(ad_account_id)
        async_job = ad_account.get_insights(params=params, is_async=True)
        report_run_id = async_job[AdReportRun.Field.id]
        # print(f"Started async query with report_run_id: {report_run_id}")
        return report_run_id

    return exponential_backoff_retry(_start)


def poll_async_job(report_run_id: str, poll_interval: int = 5, max_attempts: int = 60):
    """Poll the async job until completion or failure."""
    for attempt in range(max_attempts):
        try:
            async_job = AdReportRun(report_run_id).api_get()
            status = async_job[AdReportRun.Field.async_status]
            percent_complete = async_job[AdReportRun.Field.async_percent_completion]
            log_message(f"Status: {status}, {percent_complete}% complete")

            if status == "Job Completed":
                return async_job

            elif status in ["Job Failed", "Job Skipped"]:
                raise Exception(f"Job failed with status: {status}")

            time.sleep(poll_interval)
        except FacebookRequestError as e:
            # print(f"Error polling async job: {e.api_error_message()}")
            time.sleep(5)

    raise TimeoutError("Async job did not complete within the maximum attempts.")


def retrieve_insights_results(async_job):
    """Retrieve and return the results of a completed async job."""
    try:
        results = async_job.get_result()
        insights = [dict(insight) for insight in results]
        return insights

    except FacebookRequestError as e:
        # print(f"Failed to retrieve insights: {e.api_error_message()}")
        raise


def fetch_all_insights(ad_account_id, params, breakdowns=None, limit=100):
    """Fetch all insights with pagination, respecting the limit."""
    current_params = params.copy()
    if breakdowns:
        current_params["breakdowns"] = json.dumps(breakdowns)

    try:
        report_run_id = start_async_insights_query(ad_account_id, current_params)
        async_job = poll_async_job(report_run_id)
        insights = retrieve_insights_results(async_job)
        return insights

    except Exception as e:
        # print(f"Error fetching insights: {str(e)}")
        return None


import time
import requests

def fetch_facebook_insights_async(
    access_token: str,
    ad_account_id: str,
    params,
    max_retries: int = 6,
) -> list:
    """
    Fetch Facebook Ads Insights using the Async API with exponential backoff.

    Args:
        access_token (str): Facebook Graph API access token.
        ad_account_id (str): Ad account ID (with 'act_' prefix).
        fields (str): Comma-separated string of fields to retrieve.
        time_range (dict): Dict with 'since' and 'until' dates.
        level (str): Reporting level, default is "ad".
        limit (int): Page size for results.
        max_retries (int): Max backoff retries before failure.

    Returns:
        list: Flattened list of insights data.
    """
    base_url = f"https://graph.facebook.com/v22.0"
    
    # Step 1: Start async job
    start_resp = requests.post(
        f"{base_url}/act_{ad_account_id}/insights",
        params=params,
    )
    start_resp.raise_for_status()
    report_run_id = start_resp.json()["report_run_id"]

    # Step 2: Poll with exponential backoff
    retry = 0
    while retry < max_retries:
        status_resp = requests.get(
            f"{base_url}/{report_run_id}",
            params={"access_token": access_token},
        )
        status_resp.raise_for_status()
        status_data = status_resp.json()
        status = status_data.get("async_status")

        if status == "Job Completed":
            break
        elif status == "Job Failed":
            raise RuntimeError("Facebook async insights job failed.")
        
        wait_time = 2 ** retry  # Exponential backoff
        time.sleep(wait_time)
        retry += 1
    else:
        raise TimeoutError("Async insights job did not complete in time.")

    # Step 3: Fetch results
    results = []
    next_url = f"{base_url}/{report_run_id}/insights?access_token={access_token}"
    while next_url:
        data_resp = requests.get(next_url)
        data_resp.raise_for_status()
        data = data_resp.json()
        results.extend(data.get("data", []))
        next_url = data.get("paging", {}).get("next")

    return results


def fetch_ads_in_batches(
    ad_ids: List[str], fields: List[str] = [], batch_size: int = 50
):
    """Fetch ad data for a list of ad_ids in batches using the Ads API."""
    if not ad_ids:
        return None

    all_ads = []
    api = FacebookAdsApi.get_default_api()

    def chunks(iterable, size):
        iterator = iter(iterable)
        for first in iterator:
            yield [first] + list(islice(iterator, size - 1))

    def process_batch(batch_ad_ids):
        batch = api.new_batch()
        responses = []

        for ad_id in batch_ad_ids:
            ad = Ad(ad_id)
            ad.api_get(fields=fields, batch=batch, success=responses.append)

        batch.execute()
        time.sleep(1)

        # Collect successful responses
        return [ad_data.json() for ad_data in responses if ad_data.is_success]

    batch_counter = 0
    for batch_ad_ids in chunks(ad_ids, batch_size):
        try:
            ads = exponential_backoff_retry(lambda: process_batch(batch_ad_ids))
            all_ads.extend(ads)
            batch_counter += 1

        except Exception as e:
            log_message(f"Error processing batch {batch_counter}: {str(e)}")
            continue

    return all_ads


def process_and_insert_insights(insights_data, time_window_key, account_id, ad_id_to_skip, db: Database):
    """
    Processes fetched insights data and inserts it into the fact table.
    Discovers and upserts dimension values as they are encountered.
    Fetches account_id, adset_id, and campaign_id by joining dimension tables based on ad_id.
    Handles transaction rollback on insertion errors.
    Includes account_id, adset_id, campaign_id in INSERT and ON CONFLICT.
    Adds checks for None dimension IDs for specific breakdowns.
    Adds detailed logging for row processing errors.
    """
    if not insights_data:
        return
    
    rows_to_insert = []
    for row in insights_data:
        try:
            ad_id = int(row.get("ad_id"))
            # This will skip those ads which have creative type unknown
            if ad_id_to_skip and ad_id in ad_id_to_skip:
                continue

            adset_id = row.get("adset_id")
            campaign_id = row.get("campaign_id")

            start_date_str = row.get("date_start")
            end_date_str = row.get("date_stop")
            start_date_obj = (
                datetime.strptime(start_date_str, "%Y-%m-%d").date()
                if start_date_str
                else None
            )
            end_date_obj = (
                datetime.strptime(end_date_str, "%Y-%m-%d").date()
                if end_date_str
                else None
            )

            amount_spent = float(row.get("spend", 0))
            impressions = int(row.get("impressions", 0))
            reach = int(row.get("reach", 0))

            actions = row.get("actions", [])
            link_click = sum(
                [
                    int(action.get("value", 0))
                    for action in actions
                    if action.get("action_type") == "link_click"
                ]
            )
            landing_page_view = sum(
                [
                    int(action.get("value", 0))
                    for action in actions
                    if action.get("action_type") == "landing_page_view"
                ]
            )
            purchase = sum(
                [
                    int(action.get("value", 0))
                    for action in actions
                    if action.get("action_type") == "omni_purchase"
                ]
            )
            lead = sum(
                [
                    int(action.get("value", 0))
                    for action in actions
                    if action.get("action_type") == "lead"
                ]
            )
            post_reaction = sum(
                [
                    int(action.get("value", 0))
                    for action in actions
                    if action.get("action_type") == "react"
                ]
            )
            post_shares = sum(
                [
                    int(action.get("value", 0))
                    for action in actions
                    if action.get("action_type") == "post"
                ]
            )
            post_save = sum(
                [
                    int(action.get("value", 0))
                    for action in actions
                    if action.get("action_type") == "onsite_conversion.post_save"
                ]
            )
            video_plays_3s = sum(
                [
                    int(action.get("value", 0))
                    for action in actions
                    if action.get("action_type") == "video_view"
                ]
            )
            thruplays = sum(
                [
                    int(action.get("value", 0))
                    for action in actions
                    if action.get("action_type") == "video_thruplay_watched"
                ]
            )

            action_values = row.get("action_values", [])
            purchase_revenue = sum(
                [
                    float(av.get("value", 0))
                    for av in action_values
                    if av.get("action_type") == "omni_purchase"
                ]
            )
            lead_revenue = sum(
                [
                    float(av.get("value", 0))
                    for av in action_values
                    if av.get("action_type") == "lead"
                ]
            )

            region_id = None
            age_id = None
            gender_id = None
            platform_id = None
            placement_id = None
            impression_device_id = None

            upsert_dimension(
                db,
                "dim_account",
                "account_id",
                account_id,
                "account_id",
            )

            custom_metrics = {}

            data_tuple = (
                time_window_key,
                start_date_obj,
                end_date_obj,
                ad_id,
                account_id,
                adset_id,
                campaign_id,
                region_id,
                age_id,
                gender_id,
                platform_id,
                placement_id,
                impression_device_id,
                "account",
                amount_spent,
                video_plays_3s,
                impressions,
                reach,
                thruplays,
                link_click,
                landing_page_view,
                purchase,
                lead,
                post_reaction,
                post_shares,
                post_save,
                purchase_revenue,
                lead_revenue,
                json.dumps(custom_metrics),
                datetime.now(),
            )
            rows_to_insert.append(data_tuple)

        except Exception as e:
            log_message(
                f"\n--- Error processing insights row for Ad ID {row.get('ad_id')} ---",
                e,
            )
            traceback.print_exc()
            if isinstance(e, psy.Error):
                log_message(f"  PostgreSQL Error Code: {e.pgcode}")
                if e.diag:
                    log_message(f"  PostgreSQL Error Message: {e.diag.message_primary}")
                    try:
                        if e.diag.detail:
                            log_message(f"  PostgreSQL Error Detail: {e.diag.detail}")
                    except AttributeError:
                        pass
                    if e.diag.constraint_name:
                        log_message(
                            f"  PostgreSQL Constraint Name: {e.diag.constraint_name}"
                        )

    if rows_to_insert:
        try:
            log_message(
                f"Successfully inserted/updated {len(rows_to_insert)} rows for breakdown account."
            )
            return rows_to_insert

        except Exception as e:
            log_message(
                "\n--- Database insertion error for breakdown account ---",
                e,
            )
            traceback.print_exc()
            if isinstance(e, psy.Error):
                log_message(f"  PostgreSQL Error Code: {e.pgcode}")
                if e.diag:
                    log_message(f"  PostgreSQL Error Message: {e.diag.message_primary}")
                    try:
                        if e.diag.detail:
                            log_message(f"  PostgreSQL Error Detail: {e.diag.detail}")
                    except AttributeError:
                        pass
                    if e.diag.constraint_name:
                        log_message(
                            f"  PostgreSQL Constraint Name: {e.diag.constraint_name}"
                        )
    else:
        log_message("No valid rows to insert for breakdown account.")


def upsert_partition_atomic(new_table, rows_to_insert, db: Database):
    try:    
        # Bulk insert into the new table
        insert_stmt = f"""
            INSERT INTO {new_table} (
                time_window, start_date, end_date, ad_id, account_id, adset_id, campaign_id,
                region_id, age_id, gender_id, platform_id, placement_id, impression_device_id,
                breakdown_type,
                amount_spent, video_plays_3s, impressions, reach, thruplays,
                link_click, landing_page_view, purchase, lead, post_reaction, post_shares, post_save,
                purchase_revenue, lead_revenue,
                custom_metrics, created_at
            ) VALUES %s
                ON CONFLICT (time_window,ad_id,adset_id,campaign_id,account_id,region_id,age_id,gender_id,
                    platform_id,impression_device_id,breakdown_type,placement_id
                )
            DO UPDATE SET
                amount_spent = EXCLUDED.amount_spent,
                video_plays_3s = EXCLUDED.video_plays_3s,
                impressions = EXCLUDED.impressions,
                reach = EXCLUDED.reach,
                thruplays = EXCLUDED.thruplays,
                link_click = EXCLUDED.link_click,
                landing_page_view = EXCLUDED.landing_page_view,
                purchase = EXCLUDED.purchase,
                lead = EXCLUDED.lead,
                post_reaction = EXCLUDED.post_reaction,
                post_shares = EXCLUDED.post_shares,
                post_save = EXCLUDED.post_save,
                purchase_revenue = EXCLUDED.purchase_revenue,
                lead_revenue = EXCLUDED.lead_revenue,
                custom_metrics = EXCLUDED.custom_metrics,
                created_at = EXCLUDED.created_at;
        """
        db.bulk_execute_values(insert_stmt, rows_to_insert)
    except Exception as e:
        print(f"[DB ERROR] Atomic swap failed: {e}")
        raise e


def atomic_swap(final_table, new_table, account_id, db):
    try:
        # Detach existing partition if it exists
        db.execute(f"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM pg_class c
                    JOIN pg_partitioned_table p ON p.partrelid = 'fact_ad_metrics_aggregated'::regclass
                    JOIN pg_inherits i ON i.inhparent = p.partrelid
                    JOIN pg_class pc ON pc.oid = i.inhrelid
                    WHERE pc.relname = '{final_table}'
                ) THEN
                    EXECUTE format('ALTER TABLE fact_ad_metrics_aggregated DETACH PARTITION %I', '{final_table}');
                END IF;
            END$$;
        """)

        # Drop the old final table if needed (optional safety step)
        db.execute(f"DROP TABLE IF EXISTS {final_table}", commit=True)
        
        # Rename staging table to final table
        db.execute(f"""ALTER TABLE {new_table} RENAME TO {final_table};""", commit = True)

        # Attach the new table as a partition
        db.execute(f"""
            ALTER TABLE fact_ad_metrics_aggregated
            ATTACH PARTITION {final_table} FOR VALUES IN ({account_id});
        """, commit=True)
        
        # Optional: disable autovacuum for this partition
        db.execute(f"""ALTER TABLE {final_table} SET (autovacuum_enabled = false);""")
        db.conn.commit()
    except Exception as e:
        print(f"[DB ERROR] Atomic swap failed: {e}")
        raise e


def save_ad_creatives(all_ad_data, account_id, db: Database):
    if not all_ad_data:
        log_message("No ads found in the specified time range or account.")
        return {"message": "No ads found"}

    # --- Pass 1: Collect all unique asset IDs and hashes needing lookup ---
    image_assets_to_process: Dict[str, Dict[str, Any]] = {}
    video_assets_to_process: Dict[int, Set[int]] = {}
    all_image_hashes: Set[str] = set()
    campaigns_to_process: Dict[int, Dict[str, Any]] = {}
    adsets_to_process: Dict[int, Dict[str, Any]] = {}
    unknow_creative_ad_id = []

    for ad_data in all_ad_data:
        ad_id_str = ad_data.get("id")
        ad_id = int(ad_id_str) if ad_id_str else None

        campaign = ad_data.get("campaign", {})
        campaign_id_str = campaign.get("id")
        if campaign_id_str:
            try:
                campaign_id = int(campaign_id_str)
                if campaign_id not in campaigns_to_process:
                    campaigns_to_process[campaign_id] = {
                        "account_id": int(account_id),
                        "campaign_name": campaign.get("name", ""),
                        "objective": campaign.get("objective", ""),
                        "is_active": campaign.get("status", "") == "ACTIVE",
                    }
            except (ValueError, TypeError):
                log_message(
                    f"Invalid campaign ID format for ad {ad_id_str}: {campaign_id_str}. Skipping campaign."
                )

        adset = ad_data.get("adset", {})
        adset_id_str = adset.get("id")
        if adset_id_str and campaign_id_str:
            try:
                adset_id = int(adset_id_str)
                campaign_id = int(campaign_id_str)
                if adset_id not in adsets_to_process:
                    adsets_to_process[adset_id] = {
                        "account_id": int(account_id),
                        "campaign_id": campaign_id,
                        "adset_name": adset.get("name", ""),
                        "result_type": adset.get("optimization_goal", ""),
                        "is_active": adset.get("effective_status", "") == "ACTIVE",
                    }
            except (ValueError, TypeError):
                log_message(
                    f"Invalid adset ID format for ad {ad_id_str}: {adset_id_str}. Skipping adset."
                )

        creative_data = ad_data.get("creative", {})
        if not creative_data:
            continue

        def process_image_asset_collect(img_hash, img_url):
            if not img_hash:
                return
            all_image_hashes.add(img_hash)
            if img_hash not in image_assets_to_process:
                image_assets_to_process[img_hash] = {"url": img_url}
            elif img_url and not image_assets_to_process[img_hash]["url"]:
                image_assets_to_process[img_hash]["url"] = img_url

        def process_video_asset_collect(vid_id, current_ad_id):
            if not vid_id or current_ad_id is None:
                return
            try:
                video_id_int = int(vid_id)
                if video_id_int not in video_assets_to_process:
                    video_assets_to_process[video_id_int] = set()
                video_assets_to_process[video_id_int].add(current_ad_id)
            except (ValueError, TypeError):
                log_message(
                    f"Invalid video ID format: {vid_id} for ad {current_ad_id}. Skipping."
                )

        process_image_asset_collect(
            creative_data.get("image_hash"), creative_data.get("image_url")
        )
        process_image_asset_collect(None, creative_data.get("url"))
        process_image_asset_collect(None, creative_data.get("link_og_image_url"))

        object_story_spec = creative_data.get("object_story_spec", {})
        video_data = object_story_spec.get("video_data", {})
        if video_data:
            process_video_asset_collect(video_data.get("video_id"), ad_id)

        if object_story_spec:
            link_data = object_story_spec.get("link_data", {})
            if link_data:
                process_image_asset_collect(
                    link_data.get("image_hash"), link_data.get("image_url")
                )
                child_attachments = link_data.get("child_attachments", [])
                for card in child_attachments:
                    process_image_asset_collect(
                        card.get("image_hash"), card.get("image_url")
                    )
                    process_video_asset_collect(card.get("video_id"), ad_id)

            template_data = object_story_spec.get("template_data")
            if template_data:
                process_image_asset_collect(
                    template_data.get("image_hash"), template_data.get("image_url")
                )
                child_attachments = template_data.get("child_attachments", [])
                for card in child_attachments:
                    process_image_asset_collect(
                        card.get("image_hash"), card.get("image_url")
                    )
                    process_video_asset_collect(card.get("video_id"), ad_id)

            photo_data = object_story_spec.get("photo_data", {})
            if photo_data:
                image_info = photo_data.get("image", {})
                process_image_asset_collect(
                    image_info.get("hash"), image_info.get("url")
                )

        asset_feed_spec = creative_data.get("asset_feed_spec")
        if asset_feed_spec:
            videos = asset_feed_spec.get("videos")
            if videos and isinstance(videos, list) and len(videos) > 0:
                first_video = videos[0]
                process_video_asset_collect(first_video.get("video_id"), ad_id)

            images = asset_feed_spec.get("images")
            if images and isinstance(images, list) and len(images) > 0:
                first_image = images[0]
                process_image_asset_collect(
                    first_image.get("hash"), first_image.get("url")
                )

    log_message(
        f"Collected {len(campaigns_to_process)} unique campaigns, "
        f"{len(adsets_to_process)} unique adsets, "
        f"{len(image_assets_to_process)} unique image assets (hashes) with initial data, "
        f"{len(all_image_hashes)} total unique image hashes, and "
        f"{len(video_assets_to_process)} unique video assets (IDs)."
    )

    # --- Step 2: Batch Fetch Image URLs from Meta ---
    print("Starting batch image URL lookup for all collected hashes from Meta...")
    hash_url_map = fetch_image_urls_batch(list(all_image_hashes), account_id)

    # --- Step 3: Download Images and Upload to S3 ---
    print("Downloading images from Meta URLs and uploading to S3...")
    s3_url_map = {}
    for image_hash, meta_url in hash_url_map.items():
        if meta_url:
            s3_url = download_and_upload_image(
                image_hash, meta_url, int(ad_id_str), S3_BUCKET_NAME
            )
            if s3_url:
                s3_url_map[image_hash] = s3_url
            else:
                log_message(
                    f"Failed to download or upload image for hash {image_hash} from {meta_url}. S3 URL will be None."
                )
        else:
            log_message(
                f"No Meta URL found for hash {image_hash}. S3 URL will be None."
            )

    print(f"Successfully uploaded {len(s3_url_map)} images to S3.")

    errors_occurred = False

    try:
        for campaign_id, data in campaigns_to_process.items():
            try:
                upsert_campaign_query = """
                    INSERT INTO dim_campaign
                    (campaign_id, account_id, campaign_name, objective, is_active)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (campaign_id)
                    DO UPDATE SET
                        account_id = EXCLUDED.account_id,
                        campaign_name = EXCLUDED.campaign_name,
                        objective = EXCLUDED.objective,
                        is_active = EXCLUDED.is_active;
                """
                upsert_campaign_value = (
                    campaign_id,
                    data["account_id"],
                    data["campaign_name"],
                    data["objective"],
                    data["is_active"],
                )
                print("upsert_campaign_value", upsert_campaign_value)
                db.execute(upsert_campaign_query, upsert_campaign_value)
            except Exception as e:
                log_message(f"Error processing campaign {campaign_id}: {e}")
                errors_occurred = True

        for adset_id, data in adsets_to_process.items():
            try:
                upsert_adset_query = """
                    INSERT INTO dim_adset
                    (adset_id, account_id, campaign_id, adset_name, result_type, is_active)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (adset_id)
                    DO UPDATE SET
                        account_id = EXCLUDED.account_id,
                        campaign_id = EXCLUDED.campaign_id,
                        adset_name = EXCLUDED.adset_name,
                        result_type = EXCLUDED.result_type,
                        is_active = EXCLUDED.is_active;
                """
                upsert_adset_value = (
                    adset_id,
                    data["account_id"],
                    data["campaign_id"],
                    data["adset_name"],
                    data["result_type"],
                    data["is_active"],
                )
                db.execute(upsert_adset_query, upsert_adset_value)
            except Exception as e:
                log_message(f"Error processing adset {adset_id}: {e}")
                errors_occurred = True

        print("Processing Video Creatives...")
        for video_id, associated_ad_ids in video_assets_to_process.items():
            try:
                asyncio.run(
                    process_video_creative(
                        db, video_id, associated_ad_ids, account_id, None
                    )
                )
            except Exception as e:
                log_message(
                    f"Error running async process_video_creative for video {video_id}: {e}"
                )
                errors_occurred = True

        for image_hash in all_image_hashes:
            data = image_assets_to_process.get(image_hash, {})
            final_asset_link = s3_url_map.get(image_hash)

            if (
                process_image_creative(
                    db, image_hash, final_asset_link, data.get("tags"), account_id
                )
                is None
            ):
                errors_occurred = True

        if errors_occurred:
            log_message(
                "Errors occurred during initial dimension processing (Campaigns, Adsets, Images, Videos). Rolling back dimension updates."
            )
            db.conn.rollback()

            return {
                "status": "Failure",
                "message": "Failed to process initial dimension tables.",
            }
        else:
            db.conn.commit()
            log_message(
                "Dimension tables (Campaigns, Adsets, Images, Videos) processed and committed."
            )

    except Exception as e:
        db.conn.rollback()
        log_message(f"Critical error during initial dimension processing phase: {e}")
        return {
            "status": "Failure",
            "message": f"Critical error during initial dimension processing: {e}",
        }

    # --- Step 5: Process Ads and Carousel Cards (linking to dimensions) ---
    print("Processing Ads and Carousel Cards...")
    errors_occurred_linking = False
    update_status_failed = False
    carousel_ads_to_update_availability = set()

    try:
        for ad_data in all_ad_data:
            processed_ad_id = process_ad(db, ad_data, account_id)
            if processed_ad_id is None:
                errors_occurred_linking = True
            else:
                if isinstance(processed_ad_id, tuple):
                    processed_ad_id = processed_ad_id[0]
                    unknow_creative_ad_id.append(processed_ad_id)

                creative_data = ad_data.get("creative", {})
                if (
                    creative_data
                    and determine_creative_type(creative_data) == "carousel"
                ):
                    if process_carousel_creative(
                        db, processed_ad_id, creative_data, account_id
                    ):
                        carousel_ads_to_update_availability.add(processed_ad_id)
                    else:
                        errors_occurred_linking = True

        if errors_occurred_linking:
            log_message(
                "Errors occurred during ad/card processing. Rolling back ad/card updates."
            )
            db.conn.rollback()
            status_message = "Partial Success (Errors processing ads/cards)"
        else:
            db.conn.commit()
            log_message("Ads and Carousel Cards processed and committed.")
            status_message = "Success"

        # --- Step 6: Update Carousel Ad Availability ---
        log_message(
            f"Updating availability for {len(carousel_ads_to_update_availability)} carousel ads..."
        )
        availability_update_errors = False
        if not availability_update_errors:
            for ad_id in carousel_ads_to_update_availability:
                if not update_carousel_ad_availability(db, ad_id):
                    availability_update_errors = True
                    errors_occurred_linking = True

            if availability_update_errors:
                log_message(
                    "Errors occurred during carousel ad availability updates. Rolling back availability updates."
                )
                db.conn.rollback()
            else:
                if not errors_occurred_linking:
                    db.conn.commit()
                    log_message("Carousel ad availability updated and committed.")

        if not update_status_failed:
            try:
                update_status = """
                    INSERT INTO cron_run_status (account_id, last_run_at)
                    VALUES (%s, NOW())
                    ON CONFLICT (account_id)
                    DO UPDATE SET last_run_at = NOW(), updated_at = NOW();
                """
                db.execute(update_status, (int(account_id),), True)
                log_message("Updated cron run status.")
            except Exception as e:
                log_message(f"Error updating cron run status: {e}")
                errors_occurred_linking = True
                status_message = "Partial Success (DB error updating status)"

    except Exception as e:
        db.conn.rollback()
        log_message(f"Critical error during ad/card/availability processing phase: {e}")
        status_message = "Failure"

    finally:
        if db and db.in_transaction():
            try:
                db.conn.rollback()
            except Exception as e:
                print(f"Error during final rollback before closing session: {e}")

    return {
        "message": f"Ads and creatives fetch and processing finished. Status: {status_message}",
        "status": status_message,
        "campaigns_processed": len(campaigns_to_process),
        "adsets_processed": len(adsets_to_process),
        "all_image_hashes_collected": len(all_image_hashes),
        "video_assets_collected": len(video_assets_to_process),
        "meta_image_urls_fetched_in_batch": len(hash_url_map),
        "images_uploaded_to_s3": len(s3_url_map),
        "total_ads_retrieved": len(all_ad_data),
        "unknow_creative_ad_id": unknow_creative_ad_id,
    }


def delete_ad_and_creatives(ad_and_adset_id: List[str], account_id: int, db: Database):
    if not ad_and_adset_id:
        print("No ad_ids provided.")
        return None

    s3 = boto3.client("s3")

    try:
        # Fetch all ad details in one query
        for i in range(0, len(ad_and_adset_id), 100):
            batch = ad_and_adset_id[i : i + 100]
            ad_query = """SELECT  adset_id, ad_id, video_id, image_hash, creative_type 
                    FROM dim_ad WHERE ad_id NOT IN %s AND account_id = %s
                    LIMIT 100 OFFSET %s;
                """

            cursor = db.execute(
                ad_query,
                (
                   tuple(batch),
                    account_id,
                    100 * i,
                ),
            )
            
            ads = cursor.fetchall()

            if not ads:
                print("No matching ads found.")
                continue

            print("ads data for delete started ====================>")
            deleted_assets_campaign_ids = set()
            deleted_image_hash = set()
            deleted_video_id = set()

            # Collect creative references
            video_ids = [ad["video_id"] for ad in ads if ad["creative_type"] == "video" and ad["video_id"]]
            image_hashes = [ad["image_hash"] for ad in ads if ad["creative_type"] == "image" and ad["image_hash"]]

            # Delete from creatives
            if video_ids:
                delete_creatives_from_s3(
                    db=db,
                    s3=s3,
                    s3_prefix="meta-creatives/videos",
                    query="""
                        SELECT dvc.asset_link, dvc.video_id, da.ad_id, da.adset_id, dc.campaign_id
                        FROM dim_video_creative AS dvc
                        JOIN dim_ad AS da ON dvc.video_id = da.video_id
                        JOIN dim_adset AS dc ON da.adset_id = dc.adset_id
                        WHERE dvc.video_id IN %s AND da.account_id = %s;
                    """,
                    query_params=(
                        tuple(video_ids),
                        account_id,
                    ),
                    deleted_assets_campaign_ids=deleted_assets_campaign_ids,
                    deleted_video_id=deleted_video_id,
                )
                log_message(
                    f"video_ids deleted from s3: {','.join(map(str, video_ids))}"
                )

            if image_hashes:
                delete_creatives_from_s3(
                    db=db,
                    s3=s3,
                    s3_prefix="meta-creatives/images",
                    query="""
                        SELECT dic.asset_link, dic.image_hash, da.ad_id, da.adset_id, dc.campaign_id
                        FROM dim_image_creative AS dic
                        JOIN dim_ad AS da ON dic.image_hash = da.image_hash
                        JOIN dim_adset AS dc ON da.adset_id = dc.adset_id
                        WHERE dic.image_hash IN %s AND da.account_id = %s;
                    """,
                    query_params=(
                       tuple(image_hashes),
                        account_id,
                    ),
                    deleted_assets_campaign_ids=deleted_assets_campaign_ids,
                    deleted_image_hash=deleted_image_hash,
                )
                log_message(f"image_hashes deleted from s3: {','.join(image_hashes)}")

            if deleted_assets_campaign_ids and (deleted_image_hash or deleted_video_id):
                try:
                    # Delete from dim_campaign
                    db.execute(
                            """DELETE FROM dim_campaign WHERE campaign_id IN %s AND account_id = %s""",
                            (
                                tuple(deleted_assets_campaign_ids),
                                account_id,
                            )
                        )

                    if deleted_image_hash:
                        log_message(
                            f"==>>  Image row deleted from the database:: {','.join(map(str, deleted_image_hash))}"
                        )

                         # Delete from dim_creative_card
                        db.execute(
                            "DELETE FROM dim_creative_card WHERE image_hash IN %s AND account_id = %s",
                            (
                                tuple(deleted_image_hash),
                                account_id,
                            ),
                        )

                        # Delete from dim_image_creative
                        db.execute(
                            "DELETE FROM dim_image_creative WHERE image_hash IN %s AND account_id = %s",
                            (
                                tuple(deleted_image_hash),
                                account_id,
                            ),
                        )

                    if deleted_video_id:
                        log_message(
                            f"==>> Video row deleted from the database: {','.join(map(str, deleted_video_id))}"
                        )

                        # Delete from dim_creative_card where video_id
                        db.execute(
                            "DELETE FROM dim_creative_card WHERE video_id IN %s AND account_id = %s",
                            (
                                tuple(deleted_video_id),
                                account_id,
                            ),
                        )

                        # Delete from dim_video_creative
                        db.execute(
                            "DELETE FROM dim_video_creative WHERE video_id IN %s AND account_id = %s",
                            (
                                tuple(deleted_video_id),
                                account_id,
                            ),
                        )

                    db.conn.commit()

                except Exception as e:
                    db.conn.rollback()
                    log_message("Deletion failed:", e)
                    raise

                log_message(
                    f"Data Deleted, which was already in the database.\n Campaign Id {','.join(map(str, deleted_assets_campaign_ids))}"
                )

        return {"status": "success", "deleted_ads": len(ads)}

    except Exception as e:
        log_message("Error in delete ", e)


def delete_creatives_from_s3(
    db,
    s3,
    query,
    query_params,
    deleted_assets_campaign_ids,
    s3_prefix="meta-creatives/videos",
    deleted_image_hash=None,
    deleted_video_id=None,
):
    try:
        assets = db.execute(query, query_params).fetchall()

        for asset in assets:
            if not asset["asset_link"]:
                continue

            object_key = f"{s3_prefix}/{asset['asset_link'].split('/')[-1]}"
            s3_delete_status = s3.delete_object(Bucket=S3_BUCKET_NAME, Key=object_key)

            if (
                s3_delete_status
                and s3_delete_status.get("ResponseMetadata", {}).get("HTTPStatusCode")
                == 204
            ):
                is_video = s3_prefix.split('/')[-1] == 'videos'
                
                if not is_video:
                    deleted_image_hash.add(asset["image_hash"])

                if is_video:
                    deleted_video_id.add(asset["video_id"])

                deleted_assets_campaign_ids.add(asset["campaign_id"])

    except NoCredentialsError:
        print("AWS credentials not found.")
    except EndpointConnectionError:
        print("Could not connect to AWS region.")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "NoSuchBucket":
            print(f"Bucket '{S3_BUCKET_NAME}' does not exist.")
        elif error_code == "NoSuchKey":
            print("Object not found in bucket.")
        elif error_code == "AccessDenied":
            print("Access denied.")
        else:
            print(f"Unexpected error: {e}")
    except Exception as e:
        print(f"An unexpected exception occurred: {e}")


def filter_ads_data(access_token: str, account_id: str, days: int):
    def make_request(
        url: str,
        headers: Dict[str, str],
        params: Dict[str, Any],
        retries: int = 5,
        delay: int = 60,
        max_delay: int = 3600,
    ):
        for attempt in range(retries):
            try:
                print(f"Attempt {attempt + 1}/{retries} - Requesting data...")
                response = requests.get(url, headers=headers, params=params, timeout=90)
                response.raise_for_status()
                return response

            except requests.exceptions.Timeout as e:
                print(f"Timeout occurred: {e}")
            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response else "N/A"
                print(f"HTTP error {status}: {e}")
                if status == 429:
                    print("Rate limit hit, will retry.")
                elif 400 <= status < 500:
                    break
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}")
            except Exception as e:
                print(f"Unexpected error: {e}")

            if attempt < retries - 1:
                current_delay = min(delay * (2**attempt), max_delay)
                print(
                    f"Retrying in {current_delay // 60} minutes ({current_delay} seconds)..."
                )
                time.sleep(current_delay)

        print("Max retries reached.")
        return None

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    time_stamp = get_timestamp_from_days_ago(days)

    base_url = f"https://graph.facebook.com/v22.0/act_{account_id}/ads"
    params = {
        "fields": (
            "id,name,effective_status,"
            "campaign{id,name,objective,status},"
            "adset{id,name,optimization_goal,effective_status},"
            "creative{id,name,object_story_spec,image_url,image_hash,video_id,asset_feed_spec},"
            "updated_time"
        ),
        "filtering": json.dumps(
            [{"field": "updated_time", "operator": "GREATER_THAN", "value": time_stamp}]
        ),
        "limit": 100,
        "time_range": json.dumps({"since": "2025-01-24", "until": "2025-04-24"}),
    }

    all_ads = []
    next_url = base_url
    current_params = params
    page_count = 0

    while next_url:
        page_count += 1
        print(f"\nFetching page {page_count}...")

        response = make_request(next_url, headers, current_params)
        if not response:
            return {
                "error": f"Failed to fetch ads after retries for page {page_count}."
            }

        try:
            data = response.json()
        except json.JSONDecodeError as e:
            print(f"JSON decode error on page {page_count}: {e}")
            return {"error": f"Invalid JSON response on page {page_count}"}

        ads = data.get("data", [])
        all_ads.extend(ads)
        print(
            f"Page {page_count}: Retrieved {len(ads)} ads. Total so far: {len(all_ads)}"
        )

        paging = data.get("paging", {})
        next_url = paging.get("next")
        current_params = None

    return all_ads


def get_timestamp_from_days_ago(days_ago: int) -> int:
    target_date = datetime.now() - timedelta(days=days_ago)
    # Make sure it's in UTC and set time to midnight
    target_date = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    timestamp = int(time.mktime(target_date.timetuple()))
    return timestamp


@celery.task
def update_ad_insights():
    ACCESS_TOKEN = META_API_KEY

    db = Database()
    try:
        account_query = """SELECT account_id FROM dim_account WHERE is_active = true"""
        result = db.execute(account_query).fetchall()
        # days = [1,7,15,30,90, 180, 365]
        DAYS = 365

        for active_account in result:

            ad_id_to_skip = list()
            account_id = active_account["account_id"]
            
            timestamp = time.strftime("%Y%m%d%H%M%S")
            new_table = f"fact_acct_{account_id}_tmp_{timestamp}"
            final_table = f"fact_ad_metrics_aggregated_acct_{account_id}"
            constraint_name = f"unique_fact_keys_{account_id}_{timestamp}"

            # delete existing table
            db.execute(f"""DROP TABLE IF EXISTS {new_table}""")

            # Create unlogged staging table
            db.execute(f"""
                CREATE UNLOGGED TABLE {new_table}
                (LIKE fact_ad_metrics_aggregated INCLUDING DEFAULTS);
            """, commit=True)
            print("Table created ", new_table)
            # Drop constraint if exists
            db.execute(f"""ALTER TABLE {new_table} DROP CONSTRAINT IF EXISTS {constraint_name};""", commit=True)

            # Add composite unique constraint
            db.execute(f"""
                ALTER TABLE {new_table} 
                ADD CONSTRAINT {constraint_name} UNIQUE (time_window,ad_id,adset_id,campaign_id,account_id,region_id,age_id,gender_id,platform_id,impression_device_id,breakdown_type,placement_id);
            """, commit=True)

            log_message(f"Syncing started with account id: {account_id}")

            today_fact = date.today()
            duration = today_fact - timedelta(days=DAYS)
            FACT_FETCH_TIME_RANGE = {
                "since": duration.strftime("%Y-%m-%d"),
                "until": today_fact.strftime("%Y-%m-%d"),
            }

            try:
                # Initialize Facebook API client
                FacebookAPIClient(access_token=ACCESS_TOKEN)

                # Query parameters
                params = {
                    "access_token": ACCESS_TOKEN,
                    "level": "ad",
                    "fields": [
                        "ad_id",
                        "adset_id",
                        "campaign_id",
                        "actions",
                        "action_values",
                        "spend",
                        "impressions",
                        "reach",
                        "video_thruplay_watched_actions",
                    ],
                    "time_range": json.dumps(FACT_FETCH_TIME_RANGE),
                    "action_breakdowns": json.dumps(["action_type"]),
                    "limit": 1000,
                    "async": "true",
                }

                # Fetch insights
                print("started insigths fetch -----")
                all_insights_data = fetch_all_insights(f"act_{account_id}", params, [])
                # all_insights_data = fetch_facebook_insights_async(ACCESS_TOKEN, account_id, params)
                print(f"==>> all_insights_data: {len(all_insights_data)}")
                unique_ad_ids = set(
                    row["ad_id"] for row in all_insights_data if row.get("ad_id")
                )

                print("unique ads ids ", list(unique_ad_ids))
                delete_ad_and_creatives(list(unique_ad_ids), account_id, db)

                # Filter ads data
                ads_data = filter_ads_data(ACCESS_TOKEN, account_id, 1)
                saved_creative_data = save_ad_creatives(ads_data, account_id, db)
                ad_id_to_skip = saved_creative_data.get("unknow_creative_ad_id") or []
                last_updated_ads = set(row["id"] for row in ads_data if row.get("id"))
                ad_ids = list(unique_ad_ids - last_updated_ads)

                ads_data = fetch_ads_in_batches(
                    ad_ids,
                    [
                        "id",
                        "name",
                        "effective_status",
                        "campaign{id,name,objective,status}",
                        "adset{id,name,optimization_goal,effective_status}",
                        "creative{id,name,object_story_id,object_story_spec,image_url,image_hash,asset_feed_spec,video_id}",
                    ],
                )

                saved_creative_data = save_ad_creatives(ads_data, account_id, db)
                ad_id_to_skip.extend(saved_creative_data.get("unknow_creative_ad_id") or [])
                rows_to_insert = process_and_insert_insights(
                    all_insights_data,
                    f"{DAYS}d",
                    account_id,
                    ad_id_to_skip,
                    db,
                )
                if rows_to_insert:
                    upsert_partition_atomic(new_table, rows_to_insert, db)
                    atomic_swap(final_table=final_table, new_table=new_table, account_id=account_id, db=db)
                    db.execute(f"""DROP TABLE IF EXISTS {new_table}""", commit=True)
                    

                log_message(f"Sync completed for account id: {account_id}")

            except Exception as acc_error:
                log_message(f"Error while processing account {account_id}: {acc_error}")

    except Exception as outer_error:
        log_message(f"Fatal error during update_ad_insights: {outer_error}")


