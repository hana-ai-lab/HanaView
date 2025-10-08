import re
from playwright.sync_api import Page, expect, sync_playwright

def run(playwright):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()

    # Go to the application
    page.goto("http://localhost:8000/")

    # Wait for the PIN input fields to be visible
    expect(page.locator("#pin-inputs input").first).to_be_visible()

    # Enter the default PIN
    pin = "123456"
    for i, digit in enumerate(pin):
        page.locator(f"#pin-inputs input:nth-child({i+1})").fill(digit)

    # Click the submit button
    page.get_by_role("button", name="認証").click()

    # Wait for the dashboard to be visible
    expect(page.locator("#dashboard-content")).to_be_visible(timeout=15000)

    # Click the "World" tab
    page.get_by_role("button", name="World").click()

    # Wait for the first category header to be visible
    expect(page.locator(".world-category-header").first).to_be_visible(timeout=10000)

    # Check for a specific category header to be sure
    expect(page.get_by_role("heading", name="日本")).to_be_visible()

    # Take a screenshot for visual verification
    page.screenshot(path="jules-scratch/verification/world_tab_verification.png")
    print("Screenshot taken successfully.")

    browser.close()

with sync_playwright() as playwright:
    run(playwright)