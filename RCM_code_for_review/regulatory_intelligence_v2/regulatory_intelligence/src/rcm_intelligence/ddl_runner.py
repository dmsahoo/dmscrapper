'''
Runs the SQL files in the ddl folder of this package, with names filled in from the config.

Why: the SQL files use placeholders such as ${catalog} and ${table_pdf_catalog} instead of
real names, so the same files work in DEV, UAT and PROD. SQL cannot read the YAML config,
so this module fills in the names and runs each statement in order.

The SQL files ship inside the wheel (package data), so the notebook does not need to know
any folder path.
'''

import re
from importlib import resources

DDL_PACKAGE = 'rcm_intelligence.ddl'


def list_ddl_files(prefix=''):
    '''Names of the SQL files in the package ddl folder, in name order.'''
    names = [entry.name for entry in resources.files(DDL_PACKAGE).iterdir() if entry.name.endswith('.sql')]
    return sorted(name for name in names if name.startswith(prefix))


def statements_of(sql_text, values, file_name='sql'):
    '''Fill the placeholders, remove comments and split the text into single statements.'''
    for placeholder, value in values.items():
        sql_text = sql_text.replace(placeholder, str(value))
    left = re.findall(r'\$\{[a-z_]+\}', sql_text)
    if left:
        raise ValueError(f'{file_name}: no value in the config for {sorted(set(left))}')
    sql_text = re.sub(r'/\*.*?\*/', '', sql_text, flags=re.S)                    # remove /* block comments */
    sql_text = '\n'.join(line for line in sql_text.splitlines() if not line.strip().startswith('--'))
    return [part.strip() for part in re.split(r';\s*(?:\n|$)', sql_text) if part.strip()]


def run_ddl_files(spark, values, file_names, logger):
    '''Run every statement of the given files in order. Stops at the first error.'''
    for name in file_names:
        text = resources.files(DDL_PACKAGE).joinpath(name).read_text(encoding='utf8')
        statements = statements_of(text, values, name)
        logger.info('%s: %s statements', name, len(statements))
        for statement in statements:
            spark.sql(statement)
    return file_names
