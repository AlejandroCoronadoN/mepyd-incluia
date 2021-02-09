import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="project-etl",
    version="0.1.0",
    author="Prosperia Social",
    author_email="developers.etl@prosperia.ai",
    maintainer="Rodrigo Lara Molina",
    maintainer_email="rodrigo@prosperia.ai",
    description="A template library for procedures specific to every project",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)