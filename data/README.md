# primary_keys.json

Record the bcdata table primary keys in one spot (where available).

When finished adding a key/value pair, be sure to validate:

	python validate.py

The validation script iterates through each `table`: `column` key/value pair in `primary_keys.json`, confirming that the table exists and the given column is present within the table. Note that the script does not examine values of the column to confirm uniqueness.