import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="asynctaskgraph", # Replace with your own username
    version="0.0.1",
    author="Filip Kofron",
    author_email="filip.kofron.cz@gmail.com",
    description="Async task graph and executor.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/filipkofron/asynctaskgraph",
    packages=setuptools.find_packages(),
    install_requires=["psutil>=5.7.3"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
