from setuptools import find_packages, setup
from typing import List

hyphene_e_dot = "-e ."

def get_requirements(filepath:str)->List[str]:
    requirements = []

    with open(filepath) as file_obj:
        requirements = file_obj.readlines()
        requirements = [i.replace("\n", "")for i in file_obj]

        if hyphene_e_dot in requirements:
            requirements.remove(hyphene_e_dot)

    return requirements


setup(
    name="Machine Learning project",
    version="0.0.1",
    description="Movie Recomandation System",
    author="vamshisaideep",
    author_email="vamshisaideep@gmail.com",
    packages=find_packages(),
    install_requires = get_requirements("requirements.txt")
)


