'''Shared test helper: a copy of the config folder with the placeholders replaced by test names.'''
import os
import re
import shutil

import pytest

REPO_CONFIG = os.path.join(os.path.dirname(__file__), '..', 'config')


@pytest.fixture
def config_dir(tmp_path):
    folder = tmp_path / 'config'
    shutil.copytree(REPO_CONFIG, folder)
    for env in ('dev', 'uat', 'prod'):
        path = folder / f'common_config_{env}.yaml'
        text = path.read_text()
        # every "<placeholder, comment>" value becomes a simple valid name
        text = re.sub(r'"<([a-z\-]+)[^">]*>"', lambda m: m.group(1).replace('-', '_'), text)
        text = text.replace('"<catalog>.<schema>.<model-service>"', 'cat.sch.model')
        path.write_text(text)
    return str(folder)
