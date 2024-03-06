from setuptools import setup, find_packages
setup(
    name = 'PYTHON_BASIC',
    version = '1.0',
    packages = (
      find_packages(include = ('pythonbasic.test.mainone*', ))
      + ['prophecy_config_instances.pythonbasic.test.configs.configall']
    ),
    package_dir = {
      'prophecy_config_instances.pythonbasic.test.configs.configall': 'configs/resources/pythonbasic/test/configs/configall'
    },
    package_data = {'prophecy_config_instances.pythonbasic.test.configs.configall' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'Theano==1.0.5', 'scipy>=1.6.3,<=1.8.1', 'prophecy-libs==1.8.8'],
    entry_points = {
'console_scripts' : [
'main = pythonbasic.test.mainone.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
