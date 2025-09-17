from playwright.sync_api import sync_playwright

def run(playwright):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()
    page.goto("http://localhost:8000/frontend/index.html")
    # Wait for the AI commentary to be visible
    page.wait_for_selector(".market-section h3:has-text('AI解説')")
    page.screenshot(path="jules-scratch/verification/market_tab.png", full_page=True)

    # Click on the Nasdaq tab and take a screenshot
    page.get_by_role("button", name="Nasdaq").click()
    page.wait_for_selector("#nasdaq-commentary .ai-commentary h3:has-text('AI解説')")
    page.screenshot(path="jules-scratch/verification/nasdaq_tab.png", full_page=True)

    # Click on the SP500 tab and take a screenshot
    page.get_by_role("button", name="SP500").click()
    page.wait_for_selector("#sp500-commentary .ai-commentary h3:has-text('AI解説')")
    page.screenshot(path="jules-scratch/verification/sp500_tab.png", full_page=True)

    browser.close()

with sync_playwright() as playwright:
    run(playwright)
