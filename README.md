🌍 Global Preprint Dataset (1990–2025)

This repository provides a harmonized global dataset of preprints spanning the period 1990–2025, together with the fully reproducible pipeline used to construct it.

The dataset integrates metadata from multiple scholarly infrastructures and resolves version relationships to provide both record-level and cluster-level representations of scholarly works.

Our goal is to support large-scale research on preprints, scholarly communication, and open science by providing a transparent, reproducible, and extensible data resource.

📦 Repository Contents
Data

The repository includes several derived datasets:

Harmonized Records Dataset
Unified metadata for individual preprint records collected from multiple sources.

Parent Cluster Dataset
Aggregated representation of preprint families, where multiple versions are linked into a single parent work.

Aggregated Metrics
Summary statistics describing temporal trends, infrastructures, and dataset characteristics.

Tracker Outputs
Intermediate files used to monitor harmonization, deduplication, and clustering processes.

⚠️ Depending on repository size constraints, some large files may be distributed via external storage or release artifacts.

Code

The pipeline is organized into modular components:

Harvesting Scripts
Data acquisition from scholarly infrastructures and metadata providers.

Harmonization Pipeline
Metadata normalization and enrichment across heterogeneous sources.

Deduplication and Clustering
Algorithms for identifying related versions and grouping them into clusters.

Parent Construction
Creation of representative parent records for each work family.

Metrics Generation
Scripts to compute descriptive statistics and dataset summaries.

All steps are designed to be reproducible from raw data sources.

⭐ Key Features

Multi-infrastructure integration
Combines data from multiple preprint ecosystems.

Adaptive server identification
Flexible methods for identifying preprint servers across heterogeneous metadata.

Version clustering
Detection and linking of multiple versions of the same scholarly work.

Parent record representation
Creation of unified work-level entities from versioned records.

Fully reproducible pipeline
End-to-end workflow from raw metadata to final dataset.

Open and interoperable formats
Data provided in standard formats suitable for large-scale analysis.

🎯 Intended Uses

The dataset supports research and analysis in:

Scholarly communication studies

Bibliometrics and scientometrics

Science of Science (SciSci)

Open science and preprint policy

Research infrastructure evaluation

Reproducibility and metadata quality research

🔁 Reproducibility

All processing steps are fully documented.

The dataset can be reproduced from raw sources using the scripts provided in this repository.

The pipeline includes:

Data acquisition

Metadata harmonization

Version detection

Clustering and parent construction

Metrics generation

📖 Documentation

Additional documentation will include:

Data schema description

Pipeline methodology

Validation procedures

Usage examples

📚 Citation

Citation information will be provided upon publication of the associated manuscript.

If you use this dataset before formal publication, please cite this repository.

📜 License

License information will be added.

🤝 Contributions

Contributions, issues, and suggestions are welcome.

Please open an issue to discuss proposed improvements or extensions.