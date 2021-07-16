{{
    config(
        materialized='incremental',
		unique_key='upload_id'
    )
}}

with warehouse_log_categorization as ( select * from {{ source('clture_production_db', 'warehouse_log_categorization') }} )

SELECT 
  warehouse_log_categorization.id, 
  warehouse_log_categorization.user_id, 
  warehouse_log_categorization.upload_id, 
  warehouse_log_categorization.service, 
  warehouse_log_categorization.destination_path, 
  warehouse_log_categorization.status, 
  warehouse_log_categorization.log_timestamp, 
  warehouse_log_categorization.collection, 
  CASE WHEN warehouse_log_categorization.service :: text = 'GOOGLE' :: text THEN CASE WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'all_playlist' :: text, 'audiobook' :: text, 
    'google_mp3_other' :: text, 'instrumentals' :: text, 
    'music_library_songs' :: text, 'music_new' :: text, 
    'redemption_song' :: text, 'toutes_playlists' :: text, 
    'track' :: text]
  ) THEN 'YOUTUBE_MUSIC' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ '21_days_to_freedom' :: text, 
    'documentary' :: text, 'emotion' :: text, 
    'facebook_video' :: text, 'google_food' :: text, 
    'google_mp4_other' :: text, 'google_play_movies' :: text, 
    'library' :: text, 'liked_videos' :: text, 
    'likes' :: text, 'love' :: text, 'makeup' :: text, 
    'motion' :: text, 'nature_sounds' :: text, 
    'play_movies' :: text, 'post_malone' :: text, 
    'recipes' :: text, 'redemption_history' :: text, 
    'search_history' :: text, 'shared_album_comments' :: text, 
    'streaming_services' :: text, 'sunday_in_the_shower' :: text, 
    'sweet_november' :: text, 'vegan' :: text, 
    'vid_mp4' :: text, 'video_editing' :: text, 
    'video_reel' :: text, 'watch_history' :: text, 
    'watch_later' :: text, 'watchlist' :: text]
  ) THEN 'YOUTUBE_VIDEO' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'exercise' :: text, 'fitness' :: text, 
    'fitness_training' :: text, 'meditation' :: text, 
    'seed_health' :: text, 'workout' :: text]
  ) THEN 'GOOGLE_FIT' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'money_sends_and_requests' :: text, 
    'money_remittance_requests' :: text, 
    'money_orders_and_requests' :: text, 
    'purchase' :: text, 'purchase_history' :: text, 
    'transactions' :: text]
  ) THEN 'GOOGLE_PAY' :: text WHEN warehouse_log_categorization.collection = 'account' :: text THEN 'ACCOUNT' :: text WHEN warehouse_log_categorization.collection = 'app_usage_data_setting' :: text THEN 'APP_USAGE' :: text WHEN warehouse_log_categorization.collection = 'whisper_sync' :: text THEN 'WHISPER_SYNC' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'favorite_images' :: text, 'favorite_jobs' :: text, 
    'favorite_pages' :: text, 'favorite_places' :: text, 
    'favorite_workouts' :: text, 'favorites' :: text]
  ) THEN 'FAVORITES' :: text WHEN warehouse_log_categorization.collection = ' extensions' :: text THEN ' CHROME' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'device_history' :: text, 'devices' :: text, 
    'mobile_devices' :: text]
  ) THEN 'DEVICE' :: text WHEN warehouse_log_categorization.collection = 'my_activity' :: text THEN 'GOOGLE_ACTIVITY' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'followers' :: text, 'reviews' :: text, 
    'blogged' :: text]
  ) THEN 'GOOGLE_APP' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'data' :: text, 'data_recovery' :: text, 
    'dictionary' :: text, 'uploads' :: text]
  ) THEN 'GOOGLE_DATA' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'autofill' :: text, 'questions' :: text, 
    'questions_and_answers' :: text]
  ) THEN 'GOOGLE_FORMS' :: text WHEN warehouse_log_categorization.collection = 'hangouts' :: text THEN 'GOOGLE_HANGOUTS' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'received_jpeg ' :: text, 'resized' :: text, 
    'temp_image_for_save' :: text, 'wallpaper' :: text]
  ) THEN 'GOOGLE_IMAGES' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'e-mail_list' :: text, 'reading_list' :: text, 
    'weekly_to_do_s' :: text]
  ) THEN 'GOOGLE_LISTS' :: text WHEN warehouse_log_categorization.collection = 'location_history' :: text THEN 'GOOGLE_MAPS' :: text WHEN warehouse_log_categorization.collection = 'members' :: text THEN 'GOOGLE_MEMBERS' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'contagious' :: text, 'google_csv_other' :: text, 
    'google_json_other' :: text, 'hair' :: text, 
    'jdunston' :: text, 'memes' :: text, 
    'other_mail' :: text, 'random' :: text, 
    'speechless' :: text, 'Subnetting' :: text, 
    'suffocate' :: text, 'under' :: text, 
    'worship' :: text]
  ) THEN 'GOOGLE_OTHERS' :: text WHEN warehouse_log_categorization.collection = 'want_to_go' :: text THEN 'GOOGLE_PLACES' :: text WHEN warehouse_log_categorization.collection = 'podcast' :: text THEN 'GOOGLE_PODCASTS' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'browser_history' :: text, 'home_app' :: text, 
    'home_history' :: text, 'immediate_needs' :: text, 
    'personalized_feedback' :: text, 
    'real_estate' :: text, 'recently_viewed_discussions' :: text, 
    'recently_viewed_groups' :: text, 
    'searchengines' :: text]
  ) THEN 'GOOGLE_SEARCH_ENGINE' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'ambience' :: text, 'blocked_addresses' :: text, 
    'burn' :: text, 'businessPersonalization' :: text, 
    'daily_summaries' :: text, 'delegated_sender_addresses' :: text, 
    'forwarding_addresses' :: text, 'installs' :: text, 
    'linked_services' :: text, 'profile' :: text, 
    'ratings' :: text, 'restart' :: text, 
    'sensors' :: text, 'settings' :: text, 
    'signatures' :: text, 'soundsensing' :: text, 
    'subscription' :: text]
  ) THEN ' GOOGLE_SETTINGS' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'notification_preferences' :: text, 
    'notification_tokens' :: text]
  ) THEN 'GOOGLE_NOTIFICATIONS' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'health' :: text, 'health_and_wellness' :: text, 
    'healthy_foods_list' :: text]
  ) THEN 'HEALTH' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'animation' :: text, 'collage' :: text, 
    'google_jpg_other' :: text, 'image' :: text, 
    'snapchat_jpg' :: text, 'google_youtube_jpg_other' :: text]
  ) THEN 'IMAGE' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'ads_metadata' :: text, 'birthday_metadata' :: text, 
    'jobs_metadata' :: text, 'metadata' :: text, 
    'printer_metadata' :: text, 'series_metadata' :: text, 
    'song_metadata' :: text, 'sports_connect_metadata' :: text]
  ) THEN 'METADATA' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'april' :: text, 'august' :: text, 
    'december' :: text, 'february' :: text, 
    'january' :: text, 'july' :: text, 'june' :: text, 
    'march' :: text, 'may' :: text, 'november' :: text, 
    'october' :: text, 'september' :: text]
  ) THEN 'MONTHS' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'order_history' :: text, 'retail_customer_returns' :: text, 
    'retail_orders_returned_payments' :: text, 
    'shopping' :: text, 'shopping_list' :: text]
  ) THEN 'RETAIL' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'save-new' :: text, 'screenshot_camera' :: text, 
    'screenshot_chrome' :: text, 'screenshot_drive' :: text, 
    'screenshot_facebook' :: text, 'screenshot_gallery' :: text, 
    'screenshot_gmail' :: text, 'screenshot_instagram' :: text, 
    'screenshot_messages' :: text, 'screenshot_messenger' :: text, 
    'screenshot_nordstrom' :: text, 'screenshot_one_ui_home' :: text, 
    'screenshot_onedrive' :: text, 'screenshot_samsung_internet' :: text, 
    'screenshot_tasker' :: text, 'screenshot_twitter' :: text, 
    'screenshot_whatsapp' :: text, 'screenshot_yahoo_mail' :: text]
  ) THEN 'SCREENSHOT' :: text ELSE 'OTHER' END WHEN warehouse_log_categorization.service :: text = 'AMAZON' :: text THEN CASE WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'watch_history' :: text, 'viewing_history' :: text, 
    'prime_customer_title_relevance_recommendations' :: text, 
    'view_counts' :: text, 'search_history_click_stream' :: text, 
    'pin_setting' :: text, 'parental_control_settings' :: text, 
    'my_likes' :: text, 'library' :: text, 
    'amazon_prime_video_other' :: text, 
    'prime_location' :: text, 'prime_watchlist' :: text, 
    'series_relation' :: text, 'transcriptions' :: text]
  ) THEN 'AMAZON_PRIME_VIDEO' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'advertising_3p_audiences' :: text, 
    'advertising_advertiser_clicks' :: text, 
    'advertising_advertising_audiences' :: text, 
    'advertising_amazon_audiences' :: text, 
    'advertising_opt_out' :: text]
  ) THEN 'ADVERTISING' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'alexa_echo_hint_responses_shopping_analytics' :: text, 
    'alexa_echo_notification_opt_in_shopping_analytics' :: text, 
    'alexa_echo_notification_preferences_shopping_analytics' :: text, 
    'alexa_echo_shopping_lists_shopping_analytics' :: text, 
    'alexa_policy_instructions' :: text, 
    'away_lightning_history' :: text, 
    'away_lightning_status' :: text, 
    'hunches' :: text, 'skills' :: text]
  ) THEN 'ALEXA' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'amazon_assistant_engagement_view' :: text, 
    'amazon_assistant_feed_item_interactions' :: text, 
    'amazon_assistant_xcomp_engagement_view' :: text, 
    'amazon_assistant_xcomp_workflow_view' :: text]
  ) THEN 'AMAZON_ASSISTANT' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'app_engagement' :: text, 'app_store_click_stream_tablet_insider_enagagement' :: text, 
    'app_store_fire_tv_app_details_page' :: text, 
    'app_store_fire_tv_app_details_page_carousel' :: text, 
    'app_store_fire_tv_app_details_page_carousel_item' :: text, 
    'app_store_fire_tv_app_details_page_dialogs' :: text, 
    'app_store_game_circle_achievements' :: text, 
    'app_store_game_circle_badge_progress' :: text, 
    'app_store_game_circle_earned_experience' :: text, 
    'app_store_game_circle_game_scores' :: text, 
    'app_store_game_circle_game_time' :: text, 
    'app_store_game_circle_profile_information' :: text, 
    'app_store_game_circle_registered_devices' :: text, 
    'app_store_game_circle_show_cased' :: text, 
    'app_store_game_circle_total_experience' :: text, 
    'app_store_in_app_purchases_claim_jumper_audits' :: text, 
    'app_store_in_app_purchases_subscription_receipts' :: text]
  ) THEN 'APP_STORE' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'audbile_promotions' :: text, 
    'audible_credit' :: text, 'audible_devices_activitations' :: text, 
    'audible_gifts' :: text, 'audible_library' :: text, 
    'audible_listening' :: text, 'audible_membership_billings' :: text, 
    'audible_purchase_history' :: text, 
    'audible_send_book' :: text, 'audible_whisper_sync' :: text, 
    'audible_wishlish' :: text]
  ) THEN 'AUDIBLE' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'kindle_author_follow_author_recommendations' :: text, 
    'kindle_books_promotion_manual_reward_offers' :: text, 
    'kindle_books_promotion_reward_offer_repository' :: text, 
    'kindle_devices_kindle_application_session' :: text, 
    'kindle_devices_kindle_auto_mark_read' :: text, 
    'kindle_devices_kindle_bookmark_actions' :: text, 
    'kindle_devices_kindle_highlight_action' :: text, 
    'kindle_devices_kindle_note_actions' :: text, 
    'kindle_devices_kindle_notebook_actions' :: text, 
    'kindle_devices_kindle_quote_share_actions' :: text, 
    'kindle_devices_kindle_search_actions' :: text, 
    'kindle_devices_kindle_xray_actions' :: text, 
    'kindle_devices_reader_library_series_grouping_metrics' :: text, 
    'kindle_devices_reading_actions' :: text, 
    'kindle_devices_reading_sessions' :: text, 
    'kindle_devices_reading_session_version' :: text, 
    'kindle_devices_shows_book_cover_resume' :: text, 
    'kindle_kindle_benefits_great_on_kindle_transaction' :: text, 
    'kindle_kindle_docs_alias_device_mapping' :: text, 
    'kindle_kindle_docs_document_metadata' :: text, 
    'kindle_kindle_home_card_clicks_android' :: text, 
    'kindle_kindle_home_card_clicks_ios' :: text, 
    'kindle_kindle_home_card_views_android' :: text, 
    'kindle_kindle_home_card_views_ios' :: text, 
    'kindle_periodicals' :: text, 'kindle_reach_kindle_home_card_clicks_on_android' :: text, 
    'kindle_reach_kindle_home_card_views_on_android' :: text, 
    'kindle_reach_kindle_notifications_customer_notification_events' :: text, 
    'kindle_reach_kindle_notifications_delete_all_notification_history' :: text, 
    'kindle_reach_kindle_notifications_in_app_notifications' :: text, 
    'kindle_reach_kindle_notifications_in_app_notifications_events' :: text, 
    'kindle_reach_kindle_notifications_registered_device_endpoints' :: text, 
    'kindle_reading_insights_sessions_adjustments' :: text, 
    'kindle_web_reader_look_inside_book' :: text, 
    'book_relation' :: text, 'digital_borrows' :: text]
  ) THEN 'KINDLE' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'whole_foods_key_registration' :: text, 
    'whole_foods_lifecycle_events' :: text, 
    'whole_foods_market_orders' :: text, 
    'whole_foods_transaction_promotion_redemptions' :: text]
  ) THEN 'WHOLE_FOODS' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'retail_automotive_garage' :: text, 
    'retail_cart_items' :: text, 'retail_chat_transcripts_message_transcripts_campfire' :: text, 
    'retail_shopping_profile' :: text, 
    'retail_community_shopping_social_gamification_stats' :: text, 
    'retail_customer_login_with_amazon_transactions' :: text, 
    'retail_customer_profile_contribution_images' :: text, 
    'retail_customer_profile_contribution_visibility' :: text, 
    'retail_customer_profile_profile_attributes' :: text, 
    'retail_customer_questions_and_answers_answers_posted' :: text, 
    'retail_customer_questions_and_answers_questions_posted' :: text, 
    'retail_customer_questions_and_answers_solicitation_email_idk' :: text, 
    'retail_customer_returns' :: text, 
    'retail_customer_reviews_reviews_versions' :: text, 
    'retail_customer_reviews_verified_purchases' :: text, 
    'retail_follow' :: text, 'retail_order_history' :: text, 
    'retail_orders_returned' :: text, 
    'retail_orders_returned_payments' :: text, 
    'retail_outbound_notifications_metadata' :: text, 
    'retail_outbound_notifications_mobile_applications' :: text, 
    'retail_problems_with_orders' :: text, 
    'retail_promotions' :: text, 'retail_region_authority' :: text, 
    'retail_rental_charge_transactions' :: text, 
    'retail_rental_communications' :: text, 
    'retail_rental_contracts' :: text, 
    'retail_rental_events' :: text, 'retail_rental_extension_plans' :: text, 
    'retail_rental_items' :: text, 'retail_rental_return_shipment_items' :: text, 
    'retail_rental_return_shipments' :: text, 
    'retail_reorder_digital_dash_button' :: text, 
    'retail_reorder_digital_dash_customer_preferences' :: text, 
    'retail_reorder_digital_dash_summary' :: text, 
    'retail_website_authentication_tokens' :: text, 
    'retail_websites_traffic' :: text]
  ) THEN 'RETAIL' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'twitch_prime_account_history' :: text, 
    'twitch_prime_claimed_offers' :: text, 
    'twitch_prime_linked_twitch_accounts' :: text, 
    'twitch_prime_subscription_credit_history' :: text]
  ) THEN 'TWITCH' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'account' :: text, 'account_info' :: text]
  ) THEN 'ACCOUNT' :: text WHEN warehouse_log_categorization.collection = 'app_usage_data_setting' :: text THEN 'APP_USAGE' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'alarms' :: text, 'reminders' :: text]
  ) THEN 'ALARMS' :: text WHEN warehouse_log_categorization.collection = 'whisper_sync' :: text THEN 'WHISPER_SYNC' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'fire_tv_app_store_buy_box' :: text, 
    'fire_tv_glossary' :: text]
  ) THEN 'FIRE_TV' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'device_configuration' :: text, 
    'device_data' :: text, 'device_engagement' :: text, 
    'device_info' :: text, 'device_pairings' :: text, 
    'device_state' :: text]
  ) THEN 'DEVICE' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'subscription_history' :: text, 
    'subscription_info' :: text, 'subscription_period' :: text, 
    'subscription_recurring_deliveries_events' :: text, 
    'subscription_recurring_deliveries_orders' :: text, 
    'subscription_recurring_deliveries_subscriptions' :: text, 
    'subscription_transaction' :: text]
  ) THEN 'SUBSCRIPTION' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'smile_aggregate_customer_donation' :: text, 
    'smile_charity_selection_history' :: text, 
    'smile_customer_engagement_campaign_current_status' :: text, 
    'smile_customer_engagement_campaign_history' :: text, 
    'smile_customer_mobile_app_status_history' :: text]
  ) THEN 'SMILE' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'search_data_customer_engagement' :: text, 
    'search_data_product_metrics' :: text]
  ) THEN 'SEARCH' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'amazon_deals_orders' :: text, 
    'pay_order_details' :: text]
  ) THEN 'ORDERS' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'amazon_digital_content_ownership' :: text, 
    'camera_updates' :: text, 'gadgets' :: text, 
    'general_settings' :: text, 'settings' :: text, 
    'tablets_backup_restore' :: text, 
    'third_party_registration' :: text]
  ) THEN 'AMAZON_APP' :: text WHEN warehouse_log_categorization.collection = 'amazon_forms' :: text THEN 'AMAZON_FORMS' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'amazon_other' :: text, 'jdunston' :: text, 
    'other_business_rancor' :: text, 
    'other_data_categories_custom' :: text]
  ) THEN 'AMAZON_OTHERS' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'answer_notification_requests' :: text, 
    'answers_notifcations' :: text, 'notifications_history' :: text, 
    'notifications_subscription' :: text, 
    'outbound_notification_email_delivery_status_feedback' :: text, 
    'outbound_notification_sent_notifications' :: text, 
    'outbound_notifications_application_update_history' :: text, 
    'outbound_notifications_custom' :: text, 
    'outbound_notifications_engagment_events' :: text, 
    'outbound_notifications_push_sent_data' :: text]
  ) THEN 'AMAZON_NOTIFICATIONS' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'audio_group' :: text, 'digital_music' :: text, 
    'digital_music_track' :: text, 'historical_music_queue_items' :: text, 
    'historical_music_queues' :: text, 
    'listening' :: text, 'music_alarms' :: text, 
    'playlist' :: text, 'voice_profile_status' :: text]
  ) THEN 'AMAZON_MUSIC' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'calendars' :: text, 'dismissed' :: text, 
    'profile' :: text, 'ratings' :: text, 
    'registration' :: text, 'updates' :: text]
  ) THEN 'AMAZON_SETTINGS' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'client_events' :: text, 'timecop_config_events' :: text, 
    'touch_events' :: text, 'whitelisting_events' :: text]
  ) THEN 'AMAZON_EVENTS' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'customer_communication_experience_preferences' :: text, 
    'customer_default_provider_preferences' :: text, 
    'customer_interests' :: text, 'family_customer_preference' :: text, 
    'news_preferences' :: text, 'preferences' :: text, 
    'user_selection' :: text, 'user_stats' :: text, 
    'usersessions' :: text]
  ) THEN 'AMAZON_CUSTOMER' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'collector_list' :: text, 'guestlist' :: text, 
    'lists' :: text, 'sharable_list' :: text]
  ) THEN 'AMAZON_LIST' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'digital_action_benefit' :: text, 
    'digital_content_ownership_listening' :: text, 
    'digital_content_ownership_music_library' :: text, 
    'digital_customer_attributes' :: text, 
    'digital_order_history' :: text, 
    'digital_orders_returns' :: text, 
    'digital_redemption' :: text]
  ) THEN 'DIGITAL' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'fanshop_visits' :: text, 'feedback' :: text, 
    'pick_winner_customer_responses' :: text, 
    'responses' :: text]
  ) THEN 'AMAZON_SITE' :: text WHEN warehouse_log_categorization.collection = 'geolocation' :: text THEN 'AMAZON_MAPS' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'metadata' :: text, 'node_metadata' :: text]
  ) THEN 'METADATA' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'mobile_app' :: text, 'mobile_in_app' :: text, 
    'mobile_push_nofication_permission' :: text, 
    'mobileConsumableInApp' :: text]
  ) THEN 'AMAZON_MOBILE_APP' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'pay_browser_behavior' :: text, 
    'payment_options_externally_managed_reports' :: text, 
    'payment_options_payment_instructions' :: text, 
    'payments_payment_products' :: text]
  ) THEN 'AMAZON_PAYMENT' :: text WHEN warehouse_log_categorization.collection = ANY (ARRAY[ 'pdi' :: text, 'PDI' :: text]) THEN 'AMAZON_PDI' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'print_magazine_subscriptions' :: text, 
    'print_magazine_subscripton_change_history' :: text]
  ) THEN 'AMAZON_PRINT' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'timecop_triggers' :: text, 
    'timers' :: text]
  ) THEN 'AMAZON_TIMER' :: text WHEN warehouse_log_categorization.collection = 'product_compatibility_data_sets' :: text THEN 'AMAZON_DATA' :: text WHEN warehouse_log_categorization.collection = 'report' :: text THEN 'AMAZON_REPORT' :: text WHEN warehouse_log_categorization.collection = 'search_history_images' :: text THEN 'AMAZON_IMAGES' :: text WHEN warehouse_log_categorization.collection = 'spotify_user_details' :: text THEN 'SPOTIFY' :: text ELSE 'OTHER' END WHEN warehouse_log_categorization.service :: text = 'NETFLIX' :: text THEN CASE WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'viewing_activity' :: text, 
    'viewing_history' :: text, 'billing_history' :: text, 
    'click_stream' :: text, 'interactive_titles' :: text, 
    'playback_related_events' :: text, 
    'search_history' :: text, 'watch_history' :: text]
  ) THEN 'NETFLIX_VIDEO' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'cs_contact' :: text, 'device' :: text, 
    'indicated_preferences' :: text, 
    'ip_addresses' :: text, 'messages_sent_by_netflix' :: text, 
    'my_list' :: text, 'product_cancellation_survey' :: text, 
    'ratings' :: text, 'subscription_history' :: text]
  ) THEN 'NETFLIX_SETTINGS' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'netflix' :: text, 'netflix_other' :: text]
  ) THEN 'NETFLIX_OTHER' :: text ELSE 'OTHER' END WHEN warehouse_log_categorization.service :: text = 'HULU' :: text THEN CASE WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'viewing_history' :: text, 'streaming_history' :: text, 
    'jdunston' :: text, 'watch_history' :: text]
  ) THEN 'HULU_VIDEO' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'hulu_data' :: text, 'hulu_pdf_other' :: text, 
    'my_activity' :: text]
  ) THEN 'HULU_SETTINGS' :: text WHEN warehouse_log_categorization.collection = ANY (
    ARRAY[ 'hulu_report' :: text, 'hulu_rtk_report' :: text]
  ) THEN 'HULU_REPORT' :: text ELSE 'OTHER' END ELSE 'OTHER' END AS service_provider_type 
FROM 
  warehouse_log_categorization 


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where upload_id >= (select max(upload_id) from {{ this }})

{% endif %}