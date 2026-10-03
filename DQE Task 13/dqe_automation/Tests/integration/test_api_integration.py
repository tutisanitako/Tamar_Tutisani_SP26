"""
Integration check: crosses a system boundary (our test code <-> REST API).

Scenario - REST API: verify that user 3 has 10 posts (jsonplaceholder).
"""
import allure
import pytest
import requests


@allure.feature("REST API integration")
@pytest.mark.integration
@pytest.mark.smoke
def test_user_has_expected_post_count(api_config):
    base_url = api_config["global"]["jsonplaceholder_base_url"]
    user_id = api_config["global"]["target_user_id"]
    expected_count = api_config["global"]["expected_post_count"]

    with allure.step(f"GET {base_url}/posts?userId={user_id}"):
        response = requests.get(f"{base_url}/posts", params={"userId": user_id}, timeout=10)

    assert response.status_code == 200, f"Expected HTTP 200, got {response.status_code}"

    posts = response.json()
    assert len(posts) == expected_count, (
        f"Expected {expected_count} posts for user {user_id}, found {len(posts)}"
    )
    assert all(post["userId"] == user_id for post in posts), (
        "At least one returned post does not belong to the requested userId - "
        "API is returning cross-user data."
    )