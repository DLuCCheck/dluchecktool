
## What the bank defines
    table 1 {
        name,
        fields {
            name,
            type,
            # Checks how similar a field is to another
            check_function
        }
    }

    table 2 {
        ...
    }

    common {
        field1_name, importance
        field2_name, importance
        field3_name, importance
        ...
    }

## What the bank then should do to test
    # This function would iterate over all fields in a table
    # and check for similar fields
    model.check()
    # This function would get rows similar to `row 1` in `table 2`
    model.get_similar_rows(table 1, row 1, table 2)
    # This could be used to help train a machine learning model.




