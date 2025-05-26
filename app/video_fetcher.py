import asyncio
import os
import logging
import json
import re
from playwright.async_api import async_playwright
from typing import Optional, List

# Set up logging for this module
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def get_video_from_facebook_iframe(iframe_url: str) -> Optional[str]:
    """
    Uses Playwright to navigate a Facebook preview iframe URL,
    extract the actual video download URL by observing network requests
    and inspecting the DOM, based on the original logic provided.

    Args:
        iframe_url: The URL of the Facebook preview iframe (e.g., business.facebook.com/ads/api/preview_iframe.php?...)

    Returns:
        The extracted video download URL (e.g., an .mp4 link) if found, otherwise None.
    """
    logger.info(
        f"Attempting to fetch video URL from iframe using Playwright: {iframe_url}"
    )
    video_sources = []  # Collect potential video URLs
    # video_data = {} # Original variable, not used for return, removed

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )

            page = await context.new_page()
            await page.route("**/*", lambda route: route.continue_())
            page.on("console", lambda msg: logger.info(f"CONSOLE: {msg.text}"))

            async def capture_video_url(response):
                url = response.url

                try:
                    content_type = response.headers.get("content-type", "")
                    if "video/" in content_type or url.lower().endswith(".mp4"):
                        logger.info(
                            f" Found video response via Content-Type: {url} (Content-Type: {content_type})"
                        )
                        video_sources.append(url)
                    if (
                        "application/json" in content_type
                        or "javascript" in content_type
                        or "text/plain" in content_type
                    ):
                        try:
                            body_text = await response.text()
                            video_matches = re.findall(
                                r'(https?://[^"\']+\.mp4[^"\'\s]*)', body_text
                            )
                            for match in video_matches:
                                logger.info(
                                    f"Found video URL in response body ({content_type}): {match}"
                                )
                                video_sources.append(match)
                            playable_url_matches = re.search(
                                r'"playable_url":"([^"]+)"', body_text
                            )
                            if playable_url_matches:
                                url = playable_url_matches.group(1).replace(r"\/", "/")
                                logger.info(
                                    f"Found playable_url in response body: {url}"
                                )
                                video_sources.append(url)
                            if "videoData" in url or "video_data" in url:
                                try:
                                    json_data = json.loads(
                                        body_text
                                    )  # Use body_text from above
                                    logger.info(
                                        f"Found video data JSON response from URL match: {url}"
                                    )
                                except:
                                    pass
                        except Exception as e:
                            logger.debug(
                                f"Could not read or parse response body from {url}: {e}"
                            )

                except Exception as e:
                    logger.error(f"Error processing response URL {url}: {e}")

            page.on(
                "response",
                lambda response: asyncio.create_task(capture_video_url(response)),
            )
            logger.info(f"Navigating to iframe URL: {iframe_url}")
            await page.goto(iframe_url, timeout=90000, wait_until="load")
            logger.info("Waiting for network activity to settle...")
            await page.wait_for_load_state("networkidle", timeout=30000)
            logger.info("Searching for video elements...")
            video_elements = await page.query_selector_all("video")
            logger.info(f"Found {len(video_elements)} video elements")

            for i, video in enumerate(video_elements):
                try:
                    src = await video.get_attribute("src")
                    if src:
                        logger.info(f"Video element {i + 1} src: {src}")
                        if src.startswith("http"):
                            video_sources.append(src)
                    source_elements = await video.query_selector_all("source")
                    for source in source_elements:
                        src = await source.get_attribute("src")
                        if src:
                            logger.info(f"Source element src: {src}")
                            if src.startswith("http"):
                                video_sources.append(src)
                except Exception as e:
                    logger.error(f"Error extracting video source: {e}")

            logger.info("Checking for video data in page content...")
            try:
                content = await page.content()
                playable_url_matches = re.search(r'"playable_url":"([^"]+)"', content)
                if playable_url_matches:
                    url = playable_url_matches.group(1).replace(r"\/", "/")
                    logger.info(f"Found playable_url: {url}")
                    video_sources.append(url)

                fb_video_matches = re.findall(
                    r'(https?://[^"\']+\.mp4[^"\'\s]*)', content
                )
                for url in fb_video_matches:
                    logger.info(f"Found video URL in page: {url}")
                    video_sources.append(url)
            except Exception as e:
                logger.error(f"Error extracting video URLs from page content: {e}")

            await asyncio.sleep(5)

            try:
                js_video_sources = await page.evaluate("""() => {
                    const sources = [];
                    document.querySelectorAll('video').forEach(video => {
                        if (video.src) sources.push(video.src);
                        video.querySelectorAll('source').forEach(source => {
                            if (source.src) sources.push(source.src);
                        });
                    });
                    return sources;
                }""")
                logger.info(f"Video sources from JS: {js_video_sources}")
                video_sources.extend(js_video_sources)
            except Exception as e:
                logger.error(f"Error getting video sources via JS: {e}")

            await page.screenshot(path="facebook_iframe.png")
            logger.info("Saved screenshot to facebook_iframe.png")

            await browser.close()

            unique_sources = list(set(video_sources))
            logger.info(f"Found {len(unique_sources)} unique video sources")

            best_source = None
            for source in unique_sources:
                if source and source.lower().endswith(".mp4"):
                    best_source = source
                    break

            if not best_source and unique_sources:
                # Ensure the first source is not None before assigning
                best_source = unique_sources[0] if unique_sources[0] else None

            return best_source

    except Exception as e:
        logger.error(
            f"An error occurred during Playwright execution for iframe {iframe_url}: {e}"
        )
        return None


async def get_main_image_from_facebook_iframe(iframe_url: str) -> Optional[str]:
    """
    Extracts the main thumbnail image from a Facebook iframe URL.

    Args:
        iframe_url (str): The Facebook iframe preview URL.

    Returns:
        str or None: URL of the best thumbnail image, or None if not found.
    """
    image_sources = set()
    fb_ads_images = []
    og_image = None
    best_image = None

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                )
            )
            page = await context.new_page()

            await page.route("**/*", lambda route: route.continue_())

            async def capture_images(response):
                url = response.url
                content_type = response.headers.get("content-type", "")

                try:
                    if "image/" in content_type or re.search(
                        r"\.(jpg|jpeg|png|webp|gif)", url, re.I
                    ):
                        if "logo" not in url.lower() and "icon" not in url.lower():
                            logger.info(f"Image via network: {url}")
                            image_sources.add(url)
                            if "business.facebook.com/ads/image" in url:
                                fb_ads_images.append(url)

                    if (
                        "application/json" in content_type
                        or "javascript" in content_type
                    ):
                        body = await response.text()

                        matches = re.findall(
                            r'(https?://[^"\']+\.(jpg|jpeg|png|webp|gif))', body, re.I
                        )
                        for match in matches:
                            img_url = match[0].replace(r"\/", "/")
                            if (
                                "logo" not in img_url.lower()
                                and "icon" not in img_url.lower()
                            ):
                                image_sources.add(img_url)
                                if "business.facebook.com/ads/image" in img_url:
                                    fb_ads_images.append(img_url)

                        thumb_match = re.search(
                            r'"preferred_thumbnail":{"uri":"([^"]+)"', body
                        )
                        if thumb_match:
                            thumb_url = thumb_match.group(1).replace(r"\/", "/")
                            image_sources.add(thumb_url)
                            if "business.facebook.com/ads/image" in thumb_url:
                                fb_ads_images.append(thumb_url)

                except Exception as e:
                    logger.debug(f"Error parsing response: {url} - {e}")

            page.on(
                "response",
                lambda response: asyncio.create_task(capture_images(response)),
            )

            logger.info(f"Navigating to: {iframe_url}")
            await page.goto(iframe_url, timeout=90000, wait_until="load")
            await page.wait_for_load_state("networkidle", timeout=30000)

            # og:image tag
            try:
                og_image = await page.get_attribute(
                    'meta[property="og:image"]', "content"
                )
                if og_image:
                    image_sources.add(og_image)
                    if "business.facebook.com/ads/image" in og_image:
                        fb_ads_images.append(og_image)
            except:
                pass

            # <img> tags
            for img in await page.query_selector_all("img"):
                src = await img.get_attribute("src")
                if src and "logo" not in src.lower() and "icon" not in src.lower():
                    image_sources.add(src)
                    if "business.facebook.com/ads/image" in src:
                        fb_ads_images.append(src)

            # CSS background images
            bg_images = await page.evaluate("""() => {
                const images = new Set();
                document.querySelectorAll('*').forEach(el => {
                    const bg = window.getComputedStyle(el).backgroundImage;
                    if (bg && bg.startsWith('url(')) {
                        const url = bg.slice(4, -1).replace(/["']/g, "");
                        if (url.startsWith('http')) images.add(url);
                    }
                });
                return Array.from(images);
            }""")
            for bg in bg_images:
                image_sources.add(bg)
                if "business.facebook.com/ads/image" in bg:
                    fb_ads_images.append(bg)

            # Thumbnail-like classes
            thumb_candidates = await page.evaluate("""() => {
                const found = [];
                document.querySelectorAll('img, div').forEach(el => {
                    const cls = el.className || "";
                    const src = el.getAttribute('src');
                    const bg = window.getComputedStyle(el).backgroundImage;
                    if (/thumbnail|poster|preview|cover/i.test(cls)) {
                        if (src && src.startsWith('http')) found.push(src);
                        if (bg && bg.startsWith('url(')) {
                            found.push(bg.slice(4, -1).replace(/["']/g, ""));
                        }
                    }
                });
                return found;
            }""")
            for thumb in thumb_candidates:
                image_sources.add(thumb)
                if "business.facebook.com/ads/image" in thumb:
                    fb_ads_images.append(thumb)

            await browser.close()

            # Prefer images with /ads/image
            if fb_ads_images:
                best_image = fb_ads_images[0]
            elif og_image:
                best_image = og_image
            elif image_sources:
                # Prioritize jpgs
                jpgs = [img for img in image_sources if img.lower().endswith(".jpg")]
                best_image = jpgs[0] if jpgs else list(image_sources)[0]

            logger.info(f"✅ Best image selected: {best_image}")
            return best_image

    except Exception as e:
        logger.error(f"Error extracting image from iframe {iframe_url}: {e}")
        return None


async def get_all_images_from_facebook_iframe(iframe_url: str) -> List[str]:
    """
    Extracts all direct image URLs from a Facebook iframe, including those with initiator "image/".

    Args:
        iframe_url (str): The Facebook iframe preview URL.

    Returns:
        List[str]: All collected image URLs.
    """
    image_sources = set()

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                )
            )
            page = await context.new_page()

            # Capture network-loaded images (initiator = "image/")
            def handle_request_finished(request):
                if request.resource_type == "image":
                    url = request.url
                    if "logo" not in url.lower() and "icon" not in url.lower():
                        image_sources.add(url)
                        logger.info(f"🖼️ Image via network: {url}")

            page.on("requestfinished", handle_request_finished)

            logger.info(f"Navigating to: {iframe_url}")
            await page.goto(iframe_url, timeout=90000, wait_until="load")
            await page.wait_for_load_state("networkidle", timeout=30000)

            # Fallback: extract from DOM in case some aren't caught via network
            og_image = await page.get_attribute('meta[property="og:image"]', "content")
            if og_image:
                image_sources.add(og_image)

            for img in await page.query_selector_all("img"):
                src = await img.get_attribute("src")
                if src and "logo" not in src.lower() and "icon" not in src.lower():
                    image_sources.add(src)

            # CSS background images
            bg_images = await page.evaluate("""() => {
                const images = new Set();
                document.querySelectorAll('*').forEach(el => {
                    const bg = window.getComputedStyle(el).backgroundImage;
                    if (bg && bg.startsWith('url(')) {
                        const url = bg.slice(4, -1).replace(/["']/g, "");
                        if (url.startsWith('http')) images.add(url);
                    }
                });
                return Array.from(images);
            }""")
            image_sources.update(bg_images)

            await browser.close()

            logger.info(f"✅ Total images found: {len(image_sources)}")
            return list(image_sources)

    except Exception as e:
        logger.error(f"Error extracting images from iframe {iframe_url}: {e}")
        return []
