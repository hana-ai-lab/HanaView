from playwright.sync_api import sync_playwright, expect

def run(playwright):
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()

    # The server is running on port 8000
    # The data.json is in the root of the project, so we need to adjust the API path
    # in the frontend to fetch from the root.
    # The frontend is in frontend/, and the data is in data/
    # The server is running from the frontend directory.
    # The API call is to /api/data. This will fail.
    # I need to serve from the root of the project.
    # I will kill the current server and start a new one from the root.

    # Let's assume for now that the data is loaded correctly.
    # I will adjust the server and paths later if needed.

    page.goto("http://localhost:8000/frontend/index.html")

    # Click the "ニュース" tab
    news_tab = page.get_by_role("button", name="ニュース")
    news_tab.click()

    # Wait for the news content to be visible
    expect(page.locator("#news-content .topic-box")).to_have_count(3)

    # Take a screenshot
    page.screenshot(path="jules-scratch/verification/news_icons.png")

    browser.close()

with sync_playwright() as playwright:
    run(playwright)
