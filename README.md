Paged File (PF) component: This component provides facilities for higher-level client components to perform file I/O in terms of pages. In the PF component, methods are provided to create, destroy, open, and close paged files, to scan through the pages of a given file, to read a specific page of a given file, to add and delete pages of a given file, and to obtain and release pages for scratch use

The Record Management (RM) Component:  implement a set of functions for managing unordered files of database records. This component will rely on a Paged File (PF) component that we will provide. The Paged File component performs low-level file I/O at the granularity of pages.

The Indexing (IX) Component: implement a facility for building indexes on records stored in unordered files based on B+ trees. The Indexing component will rely on the Paged File component.

The System Management (SM) Component:  implement various database and system utilities, including data definition commands and catalog management. The System Management component will rely on the Record Management and Indexing components from Parts 1 and 2. It also will use a command-line parser, which we will provide.

The Query Language (QL) Component: consists of user-level data manipulation commands, both queries and updates. The Query Language component will rely on the three components from Parts 1-3, and it will use the command-line parser that we are providing.
