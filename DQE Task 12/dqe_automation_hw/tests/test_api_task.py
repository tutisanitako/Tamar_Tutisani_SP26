import allure
import pytest


@allure.feature("API Testing")
@allure.story("JSONPlaceholder posts")
def test_user_with_posts(provide_posts_data):
    with allure.step("Fetch posts for user 3"):
        posts = provide_posts_data

    with allure.step("Verify exactly 10 posts exist for user 3"):
        assert len(posts) == 10, f"Expected 10 posts, got {len(posts)}"

    with allure.step("Verify post ids are the expected 21-30 range"):
        ids = sorted(post["id"] for post in posts)
        assert ids == list(range(21, 31)), f"Unexpected id range: {ids}"

    with allure.step("Verify each post belongs to user 3 and has non-empty title/body"):
        for post in posts:
            assert post["userId"] == 3
            assert isinstance(post["title"], str) and len(post["title"]) > 0
            assert isinstance(post["body"], str) and len(post["body"]) > 0


@allure.feature("Cloud Storage Reconciliation")
@allure.story("GCP source vs AWS staging smoke test")
def test_data_is_presented_between_staging_raw(list_gcs_blobs, list_aws_blobs):
    with allure.step("Check GCP source bucket is not empty for the given date"):
        assert len(list_gcs_blobs) > 0, "No objects found in GCP source bucket"

    with allure.step("Check AWS target bucket is not empty for the given date"):
        assert len(list_aws_blobs) > 0, "No objects found in AWS staging bucket"