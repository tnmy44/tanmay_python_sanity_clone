from setuptools import setup, find_packages
setup(
    name = 'SamplingModel_DisabledAll',
    version = '1.0',
    packages = find_packages(include = ('pythonbasic.test.mainone*', )) + ['prophecy_config_instances'],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'Theano==1.0.5', 'scipy>=1.6.3,<=1.8.1', 'prophecy-libs==1.8.7'],
    entry_points = {
'console_scripts' : [
'main = pythonbasic.test.mainone.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
