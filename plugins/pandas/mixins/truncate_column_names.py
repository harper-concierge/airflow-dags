import pandas as pd


class TruncateColumnNamesMixin:
    def _parts_length(self, parts, seperator):
        # Calculate the total length of preserved array values plus 1 for each element
        return sum(len(part) + len(seperator) for part in parts)

    def _truncate_name(self, name, max_length, seperator, joiner="", preserve_parts=0):
        if len(name) <= max_length:
            return name

        # First split by double underscore to get the main parts
        main_parts = name.split(seperator)

        # Keep first part in full
        first_part = main_parts[0]
        last_part = main_parts[-1]

        if len(main_parts) > 2:
            # We have middle parts to abbreviate
            middle_parts = main_parts[1:-1]

            # For each middle part, preserve structure while abbreviating
            abbreviated_middle = []
            for part in middle_parts:
                # Split by double underscore first to preserve structure
                subparts = part.split(seperator)
                abbreviated_subparts = []

                for subpart in subparts:
                    # Split by single underscore and abbreviate
                    words = subpart.split("_")
                    abbreviated_words = [w[:3] for w in words]
                    # Join back with single underscore
                    abbreviated_subparts.append("_".join(abbreviated_words))

                # Join subparts back with double underscore
                abbreviated_middle.append(seperator.join(abbreviated_subparts))

            result = f"{first_part}__{seperator.join(abbreviated_middle)}__{last_part}"
        else:
            # Only two parts, keep them as is
            result = f"{first_part}__{last_part}"

        return result

    def squash_column_names(self, df, max_prefix_length=25, max_suffix_length=38):
        new_columns = []
        total_max_length = max_prefix_length + max_suffix_length

        for col in df.columns:
            print(f"my col '{col}'", col)
            col = col.strip()
            if col.endswith("__v"):
                print(f"Replacing __v column {col} to m_version")
                col = col.replace("__v", "m_version")
            if col.endswith("___id"):
                print(f"Replacing _id column {col} to m_version")
                col = col.replace("___id", "__id")
            print(f"Resulting column name is {col} {len(col)}")

            # Just use the total max length for all columns
            new_col = self._truncate_name(col, total_max_length, "__", "_", 0)
            print(f"{col} --> {new_col} ({len(new_col)})")
            new_columns.append(new_col)
        df.columns = new_columns
        return df


class SquashableDataFrame(TruncateColumnNamesMixin, pd.DataFrame):
    @property
    def _constructor(self):
        return SquashableDataFrame
