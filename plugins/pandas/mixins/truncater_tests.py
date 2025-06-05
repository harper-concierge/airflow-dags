import pandas as pd

from plugins.pandas.mixins.truncate_column_names import SquashableDataFrame


class TestTruncateColumnNames:
    @staticmethod
    def run_tests():
        # Example data with multiple DataFrames
        data = [
            {
                "feature_flags__outbound_webhooks__concierge_complete_webhook__auth": [1, 2],
                "platform__centra__services__harper_concierge__api_key_with_extra_long_suffix_that_exceeds_limit": [  # noqa
                    1,
                    2,
                ],
                "platform__centra__services__harper_concierge__api_key_with_extra_long_suffix_that_exceeds_limit": [  # noqa
                    3,
                    4,
                ],
                "platform__centra__services__harper_try__available_services__api_key_with_extra_long_suffix_that_exceeds_limit": [  # noqa
                    5,
                    6,
                ],
                "platform__centra__services__harper_try__available_services__api_key_with_extra_long_suffix_that_exceeds_limit": [  # noqa
                    5,
                    6,
                ],
                "services__harper_concierge__service_settings__shopify__api_key_with_extra_long_suffix": [  # noqa
                    7,
                    8,
                ],
                "services__harper_try__service_settings__createdat": [
                    7,
                    8,
                ],
                "services__harper_concierge__service_settings____v": [11, 12],
                "schedule__day_0__after_offset__available_from": [11, 12],
            },
        ]

        # Test with each DataFrame
        for idx, df_data in enumerate(data, start=1):
            print(f"\n=== Test Case {idx} ===")
            df = pd.DataFrame(df_data)

            # Print original columns
            print("\nOriginal Columns:")
            print(df.columns)

            # Apply the mixin
            squashed_df = SquashableDataFrame(df)
            squashed_df = squashed_df.squash_column_names(df)
            print("\nSquashed Columns:")
            print(squashed_df.columns)


# Run the tests
TestTruncateColumnNames.run_tests()
