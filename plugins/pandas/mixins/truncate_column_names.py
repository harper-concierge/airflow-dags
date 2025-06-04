import pandas as pd


class TruncateColumnNamesMixin:
    def _parts_length(self, parts, seperator):
        # Calculate the total length of preserved array values plus 1 for each element
        return sum(len(part) + len(seperator) for part in parts)

    def _abbreviate_middle_words(self, name, seperator):
        """Helper method to abbreviate middle words while keeping first and last words full."""
        parts = name.split(seperator)
        abbreviated_parts = []
        for part in parts:
            words = part.split("_")
            abbreviated_words = []
            for i, word in enumerate(words):
                if i == 0 or i == len(words) - 1:
                    # Keep first and last words in full
                    abbreviated_words.append(word)
                else:
                    # Abbreviate middle words
                    abbreviated_words.append(word[:3] if len(word) > 3 else word)
            abbreviated_parts.append("_".join(abbreviated_words))
        result = seperator.join(abbreviated_parts)
        return result

    def _truncate_name(self, name, max_length, seperator, joiner="", preserve_parts=0):
        if len(name) <= max_length:
            return name

        parts = name.split(seperator)
        preserved = []

        parts.reverse()  # reverse because pop() is efficient pop(0) is not.
        if preserve_parts:
            for _ in range(preserve_parts):
                if len(parts) > 0:
                    preserved.append(parts.pop())  # Remove and add to preserved

        remaining_length = self._parts_length(parts, seperator)
        preserved_length = self._parts_length(preserved, seperator)
        abbreviated_length = 0

        parts_left = len(parts)
        abbreviated = []
        while parts_left and (remaining_length + preserved_length + abbreviated_length + len(seperator)) > max_length:
            current_part = parts.pop()
            if "_" in current_part:
                sub_parts = current_part.split("_")
                abbreviation = "".join(part[0] for part in sub_parts)
                print("abbreviated length", abbreviation, abbreviated, len(abbreviated))
            else:
                abbreviation = current_part[0]

            abbreviated.append(abbreviation)

            abbreviated_length = self._parts_length(abbreviated, joiner)
            remaining_length = self._parts_length(parts, seperator)
            preserved_length = self._parts_length(preserved, seperator)
            parts_left = len(parts)
            print(
                "INTERIM",
                parts_left,
                abbreviated,
                abbreviated_length,
                parts,
                remaining_length,
                preserved,
                preserved_length,
                len(seperator),
                (abbreviated_length + remaining_length + preserved_length + len(seperator)),
            )
        print("parts_left", parts_left)

        if (abbreviated_length + remaining_length + preserved_length + len(seperator)) > max_length:
            try:
                return self._abbreviate_middle_words(name, seperator)
            except Exception:
                raise ValueError(f"Could Not truncate Field {name} as the final part is just too long for its prefix")

        print("pre-final abbreviated", abbreviated)
        # abbreviated.reverse()
        # if abbreviated and isinstance(abbreviated[-1], str) and abbreviated[-1].endswith("_"):
        #   abbreviated[-1] = abbreviated[-1][:-1]  # Replace the last element's _ if it ends with _

        print("final abbreviated", abbreviated)

        parts.append(joiner.join(abbreviated))
        parts.reverse()

        preserved.extend(parts)

        result = f"{seperator}".join(preserved)
        if len(result) > max_length:
            print("parts", parts, parts_left, remaining_length)
            print("abbreviated", abbreviated, abbreviated_length)
            print("preserved", preserved, remaining_length)
            print("lengths", remaining_length, preserved_length, abbreviated_length, max_length)
            try:
                return self._abbreviate_middle_words(name, seperator)
            except Exception:
                raise ValueError(f"Could Not truncate Field {name} as the final part is just too long for its prefix")
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
            if len(col) <= total_max_length:
                new_col = col
            elif "__" in col:
                prefix, suffix = col.rsplit("__", 1)
                truncated_prefix = self._truncate_name(prefix, max_prefix_length, "__", "_", 1)

                available_suffix_length = total_max_length - len(truncated_prefix) - len("__")
                available_suffix_length = max(0, available_suffix_length)
                print("prefix", truncated_prefix)
                print("available_suffix_length", available_suffix_length)

                truncated_suffix = self._truncate_name(suffix, available_suffix_length, "_", "", 0)

                new_col = f"{truncated_prefix}__{truncated_suffix}"
            else:
                new_col = self._truncate_name(col, total_max_length, "_", 1)
            print(f"{col} --> {new_col} ({len(new_col)})")
            new_columns.append(new_col)
        df.columns = new_columns
        return df


class SquashableDataFrame(TruncateColumnNamesMixin, pd.DataFrame):
    @property
    def _constructor(self):
        return SquashableDataFrame
