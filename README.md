# Network_Attached_Storage Project Description:
Keywords: Public Cloud Services, Replication, Vendor Lock-in, Object Storage

In this project, we will be designing a NAS (network-attached storage) backed by three public 
cloud storage services. These cheap, scalable, and highly reliable cloud storage services are 
available for both internal use of the cloud and external access of client-side applications. The variety 
and convenience of cloud storage give users huge leverage when developing web-connected 
applications.
From the perspective of customers, a single cloud provider may not be trustworthy enough for 
providing the best quality of service and cost-efficiency. A single-platform application suffers the 
“vendor lock-in” problem, blocking the customers from migrating to better cloud providers. More and 
more customers have adopted a multi-platform approach, in order to obtain better services. 

RAID-on-Cloud, a cloud-based replication NAS which will be implemented in this project. 
The client-end will be implemented in Python, as a CLI (command-line interface) running in a Singularity Container backed by TAMU’s High-Performance Research Center (HPRC). 
Through the Python CLI, users can access files without knowing how data are actually stored and distributed across cloud platforms.
The files stored in RAID-on-Cloud will be replicated across three cloud providers, AWS S3, Azure Blob Storage, and Google Cloud Storage, in a RAID-5 style (without parity bits).
The minimum RAID-5 setup requires at least three storage media, with each block replicated to at least two locations. 
The file blocks must be stored as objects in the cloud storage services, and the storage sizes in all three cloud storage services must be equally distributed.

The CSCE678 Spring 2021- Design NAS.pdf explains the project, implementation and steps in detail
