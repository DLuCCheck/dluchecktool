## What is DLucheck
DLucheck is a table tool automated checking tool

## How to use
DLucheck is supposed to be used with table information and checks to be done to
the table, the checks are sql queries. Additional logic can be added to then work
with the data after the queries.


## Table info format
Table info can be provided via a json file. Said json must look like this
```
"<table name>": {
    "<field name>": {
        /* Field's type, must be a valid sql type(e.g., text, integer, real) */
        "type": "text",

        /* Whether the field is a pickfield or not */
        "is_pickfield": false

        /* All the valid field values, only applies if field is pickfield */
        "choices": []
    }
}
```

## Checks format
```
"{
    "<check name>": {
        /* Whether this check must be run over all the table rows */
        "repeat": false,

        /*
        Select query to be performed
        the brackets `{}` in the query refer to the value of the current field
        */
        "query": "\"Country\" = {} AND \"Region\" = {}"

        /*
        Success function can be one of:
            - NO:
                No success measurement

            - ANY:
                1 if the result of the query contains 1 or more rows
                0 otherwise

            - NONE:
                1 if the result of the query contains 0 rows 1 otherwise.

            - CUSTOM(): <In construction>
                custom python function
        */
        "success": "NO"

        /* function which returns a string to print as the result of the check */
        "label": "results"
    }
}
```

## The plan
As of yet the project is not finished, the plan is to make a graphical interface
were an end user could easily set table information for tables and create
`checks` and `queries` with a simple yet flexible UI, additionally, adding a way
to run checks over multiple tables would be desired.
