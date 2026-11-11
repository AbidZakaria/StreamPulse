"""Capture native-resolution screenshots of every dashboard view.

Uses Playwright with an installed Chromium-family browser (Edge or Chrome, no browser
download needed). The stack must be running (``docker compose up``).

    pip install playwright
    python scripts/capture_dashboard.py --out docs/images --channel msedge
"""

from __future__ import annotations

import argparse
from pathlib import Path

from playwright.sync_api import Page, sync_playwright


def settle(page: Page, selector: str, ms: int = 1500) -> None:
    page.wait_for_selector(selector, timeout=20000)
    page.wait_for_timeout(ms)


def capture_desktop(page: Page, base: str, out: Path, device: str) -> None:
    page.goto(f"{base}/#overview")
    settle(page, "#ov-titleblock div")
    page.screenshot(path=out / "desktop-overview.png")

    page.goto(f"{base}/#pipeline")
    settle(page, "#pl-rate svg")
    page.screenshot(path=out / "desktop-pipeline.png")
    page.screenshot(path=out / "desktop-pipeline-full.png", full_page=True)

    page.goto(f"{base}/#devices/{device}")
    settle(page, "#dv-charts svg", 2500)
    page.screenshot(path=out / "desktop-devices.png")
    page.screenshot(path=out / "desktop-devices-full.png", full_page=True)
    # The dashboard redraws every 5 s, which can detach the element mid-hover: retry.
    for _ in range(4):
        balloon = page.locator("#dv-detail .balloon").first
        if not balloon.count():
            break
        try:
            balloon.scroll_into_view_if_needed(timeout=3000)
            # Move the real pointer onto the numbered circle (Playwright's actionability
            # checks are unreliable on SVG groups); then a viewport shot, because an element
            # shot would scroll and move the pointer away.
            box = balloon.locator("circle:not(.onset)").bounding_box()
            page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
            page.wait_for_timeout(400)
            page.screenshot(path=out / "desktop-balloon-hover.png")
            break
        except Exception:  # noqa: BLE001
            page.wait_for_timeout(1000)

    page.goto(f"{base}/#alerts")
    settle(page, "#al-table tbody tr[data-alert-id]")
    open_row = page.locator("#al-table tbody tr:has(.status-open)").first
    (open_row if open_row.count() else page.locator("#al-table tbody tr[data-alert-id]").first).click()
    settle(page, "#al-detail .readings")
    page.evaluate("window.scrollTo(0, 0)")
    page.screenshot(path=out / "desktop-alerts.png")
    page.screenshot(path=out / "desktop-alerts-full.png", full_page=True)

    page.goto(f"{base}/#simulator")
    settle(page, "#sim-state dt")
    page.screenshot(path=out / "desktop-simulator.png")


def capture_mobile(page: Page, base: str, out: Path, device: str) -> None:
    for view, sel in (("overview", "#ov-titleblock div"), (f"devices/{device}", "#dv-charts svg"),
                      ("alerts", "#al-table tbody tr[data-alert-id]")):
        page.goto(f"{base}/#{view}")
        settle(page, sel, 2000)
        name = view.split("/")[0]
        page.screenshot(path=out / f"mobile-{name}.png")
        page.screenshot(path=out / f"mobile-{name}-full.png", full_page=True)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="http://localhost:8080")
    p.add_argument("--out", type=Path, default=Path("docs/images"))
    p.add_argument("--channel", default="msedge", help="msedge | chrome")
    p.add_argument("--device", default="sensor_001")
    args = p.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as pw:
        browser = pw.chromium.launch(channel=args.channel)
        desktop = browser.new_page(viewport={"width": 1440, "height": 900}, device_scale_factor=1)
        capture_desktop(desktop, args.base, args.out, args.device)
        mobile = browser.new_page(viewport={"width": 390, "height": 844}, device_scale_factor=2,
                                  is_mobile=True, has_touch=True)
        capture_mobile(mobile, args.base, args.out, args.device)
        browser.close()
    print("captured into", args.out)


if __name__ == "__main__":
    main()
