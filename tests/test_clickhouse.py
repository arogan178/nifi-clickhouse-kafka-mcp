import pytest

from nifi_clickhouse_kafka_mcp.clickhouse import check_ast, param_value

# EXPLAIN AST output captured from ClickHouse 26.3.
SELECT_URL = """SelectWithUnionQuery (children 1)
 ExpressionList (children 1)
  SelectQuery (children 2)
   ExpressionList (children 1)
    Function upper (children 1)
     ExpressionList (children 1)
      Identifier x
   TablesInSelectQuery (children 1)
    TablesInSelectQueryElement (children 1)
     TableExpression (children 1)
      Function url (children 1)
       ExpressionList (children 2)
        Literal 'http://a'
        Identifier CSV"""

NUMBERS_JOIN_FILE = """SelectWithUnionQuery (children 1)
 ExpressionList (children 1)
  SelectQuery (children 2)
   ExpressionList (children 1)
    Asterisk
   TablesInSelectQuery (children 2)
    TablesInSelectQueryElement (children 1)
     TableExpression (children 1)
      Function numbers (children 1)
       ExpressionList (children 1)
        Literal UInt64_3
    TablesInSelectQueryElement (children 2)
     TableExpression (children 1)
      Function file (children 1)
       ExpressionList (children 1)
        Literal '/etc/passwd'
     TableJoin (children 1)"""

S3_IN_SUBQUERY = """SelectWithUnionQuery (children 1)
 ExpressionList (children 1)
  SelectQuery (children 3)
   Function in (children 1)
    ExpressionList (children 2)
     Literal UInt64_1
     Subquery (children 1)
      SelectWithUnionQuery (children 1)
       ExpressionList (children 1)
        SelectQuery (children 2)
         TablesInSelectQuery (children 1)
          TablesInSelectQueryElement (children 1)
           TableExpression (children 1)
            Function s3 (children 1)
             ExpressionList (children 1)
              Literal 'x'"""

PLAIN_SELECT = """SelectWithUnionQuery (children 1)
 ExpressionList (children 1)
  SelectQuery (children 2)
   ExpressionList (children 1)
    Function count (children 1)
   TablesInSelectQuery (children 1)
    TablesInSelectQueryElement (children 1)
     TableExpression (children 1)
      TableIdentifier shop.orders"""

NUMBERS_ONLY = """SelectWithUnionQuery (children 1)
 ExpressionList (children 1)
  SelectQuery (children 2)
   TablesInSelectQuery (children 1)
    TablesInSelectQueryElement (children 1)
     TableExpression (children 1)
      Function numbers (children 1)
       ExpressionList (children 1)
        Literal UInt64_3"""


@pytest.mark.parametrize(
    "ast",
    [
        PLAIN_SELECT,
        NUMBERS_ONLY,
        "Explain EXPLAIN ESTIMATE (children 1)\n SelectWithUnionQuery (children 1)",
        "ShowTables (children 1)\n Identifier shop",
        "ExistsTableQuery shop orders (children 2)",
    ],
)
def test_allows_reads(ast):
    check_ast(ast)


@pytest.mark.parametrize(
    "ast, message",
    [
        ("InsertQuery   (children 2)\n Identifier t", "InsertQuery"),
        ("KillQueryQuery Literal_UInt64_1 ASYNC (children 1)", "KillQueryQuery"),
        ("AlterQuery (children 1)", "AlterQuery"),
        ("", "allowed"),
        (SELECT_URL, "url"),
        (NUMBERS_JOIN_FILE, "file"),
        (S3_IN_SUBQUERY, "s3"),
        ("DescribeQuery (children 1)\n TableExpression (children 1)\n  Function url (children 1)", "url"),
    ],
)
def test_refuses_writes_and_external_table_functions(ast, message):
    with pytest.raises(ValueError, match=message):
        check_ast(ast)


CLUSTER_URL = """SelectWithUnionQuery (children 1)
 ExpressionList (children 1)
  SelectQuery (children 2)
   TablesInSelectQuery (children 1)
    TablesInSelectQueryElement (children 1)
     TableExpression (children 1)
      Function cluster (children 1)
       ExpressionList (children 2)
        Literal 'default'
        Function url (children 1)
         ExpressionList (children 1)
          Literal 'http://a'"""

SCALAR_FILE = """SelectWithUnionQuery (children 1)
 ExpressionList (children 1)
  SelectQuery (children 1)
   ExpressionList (children 1)
    Function file (children 1)
     ExpressionList (children 1)
      Literal '/etc/passwd'"""

EXPLAIN_INSERT = """Explain EXPLAIN PIPELINE (children 1)
 InsertQuery   (children 2)
  Function url (children 1)"""

SCALAR_FORMAT = SCALAR_FILE.replace("Function file", "Function format")


@pytest.mark.parametrize("ast, message", [(CLUSTER_URL, "url"), (SCALAR_FILE, "file"), (EXPLAIN_INSERT, "InsertQuery")])
def test_refuses_nested_scalar_and_wrapped_forms(ast, message):
    with pytest.raises(ValueError, match=message):
        check_ast(ast)


def test_scalar_format_is_fine():
    check_ast(SCALAR_FORMAT)


def test_extra_table_functions_can_be_allowed():
    check_ast(SELECT_URL, frozenset({"url"}))


def test_scalar_functions_are_not_table_functions():
    # upper() sits outside any TableExpression, so it is never checked.
    check_ast(SELECT_URL.replace("Function url", "Function numbers"))


@pytest.mark.parametrize(
    "value, encoded",
    [
        ("C:\\new\\table", "C:\\\\new\\\\table"),
        ("a\tb\nc", "a\\tb\\nc"),
        (None, "\\N"),
        (True, "true"),
        (7, "7"),
        (["x", "it's", None, 3], "['x','it\\'s',NULL,3]"),
    ],
)
def test_param_values_use_clickhouse_text_format(value, encoded):
    assert param_value(value) == encoded


HAS_COLUMN = """SelectWithUnionQuery (children 1)
 ExpressionList (children 1)
  SelectQuery (children 1)
   ExpressionList (children 1)
    Function hasColumnInTable (children 1)
     ExpressionList (children 6)
      Literal 'attacker:9999'"""

NUMBERS_WITH_SCALAR_ARG = """SelectWithUnionQuery (children 1)
 ExpressionList (children 1)
  SelectQuery (children 2)
   TablesInSelectQuery (children 1)
    TablesInSelectQueryElement (children 1)
     TableExpression (children 1)
      Function numbers (children 1)
       ExpressionList (children 1)
        Function plus (children 1)"""

EXPLAIN_WITH_SETTINGS = """Explain EXPLAIN PIPELINE (children 2)
 Set
 SelectWithUnionQuery (children 1)
  ExpressionList (children 1)"""


def test_refuses_scalar_functions_that_reach_outside():
    with pytest.raises(ValueError, match="hasColumnInTable"):
        check_ast(HAS_COLUMN)


def test_without_the_server_list_functions_in_from_must_be_allowlisted():
    check_ast(NUMBERS_WITH_SCALAR_ARG)
    with pytest.raises(ValueError, match="plus"):
        check_ast(NUMBERS_WITH_SCALAR_ARG, strict=True)
    with pytest.raises(ValueError, match="icebergCluster"):
        check_ast(CLUSTER_URL.replace("Function url", "Function icebergCluster"), strict=True)


def test_explain_settings_are_skipped_to_find_the_statement():
    check_ast(EXPLAIN_WITH_SETTINGS)
    with pytest.raises(ValueError, match="InsertQuery"):
        check_ast(EXPLAIN_WITH_SETTINGS.replace(" SelectWithUnionQuery", " InsertQuery"))
