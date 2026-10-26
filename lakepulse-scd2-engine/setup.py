from setuptools import setup, find_packages

setup(
    name="lakepulse_scd2_engine",
    version="1.0.0",
    author="Prem Prakash",
    description="Metadata-driven FinTech REST API to Silver SCD Type 2 Lakehouse Engine on Databricks",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "requests>=2.31.0",
    ],
    python_requires=">=3.10",
)
