# Architecture
Everything you need to know about the architecture.
I believe strongly that documentation should be maintained, in some way, as a core feature of a product or tool alongside its codebase.
Latest version can be viewed [here](https://github.com/mxava/mcbroken-ml/blob/main/README.md)

## SVC Relationship Diagram
```mermaid
erDiagram
    PUBLIC_INTERNET }o--|| K8S_CLUSTER : inbound_connections
    K8S_CLUSTER ||--|{ FRONTEND_SVC : hosts
    K8S_CLUSTER ||--|{ ML_MODEL : hosts
    K8S_CLUSTER ||--|{ BACKEND_QUEUE_SVC : hosts
    K8S_CLUSTER ||--|{ POSTGRESQL_DB : hosts
    FRONTEND_SVC }|--|{ ML_MODEL : accesses
    ML_MODEL }|--|{ POSTGRESQL_DB : accesses
    ML_MODEL }|--|{ BACKEND_QUEUE_SVC : accesses
    BACKEND_QUEUE_SVC }|--|{ POSTGRESQL_DB : accesses
    DEV_TOOLS ||--|{ BACKEND_QUEUE_SVC : accesses
    DEV_TOOLS ||--|| K8S_CLUSTER : accesses
```

## SVC Sequence Diagram - Frontend
```mermaid
sequenceDiagram
   participant PUBLIC_INTERNET
   participant FRONTEND_SVC
   participant ML_MODEL
   participant SQL_DB
   participant BACKEND_SVC

   PUBLIC_INTERNET->>FRONTEND_SVC: sends request
   FRONTEND_SVC->>ML_MODEL: sends request
   ML_MODEL-->>BACKEND_SVC: check queue status<br/>waits as needed
   BACKEND_SVC-->>ML_MODEL: return response
   ML_MODEL->>SQL_DB: query database
   SQL_DB->>ML_MODEL: return query
   ML_MODEL->>FRONTEND_SVC: return response
   FRONTEND_SVC->>PUBLIC_INTERNET: return response
```

## SVC Sequence Diagram - Backend
### Queue Handling
```mermaid
sequenceDiagram
   participant QUEUE
   participant BACKEND_SVC
   participant SQL_DB

   Note over BACKEND_SVC: orders queue by ascending time_modified
   QUEUE-->>BACKEND_SVC: fetches lowest item in queue
   Note over BACKEND_SVC,SQL_DB: does not not overwrite where<br/>DB last_modified >= queue time_modified<br/>except when annotating ML data
   BACKEND_SVC->>SQL_DB: updates database
   SQL_DB->>BACKEND_SVC: transaction completes
   BACKEND_SVC->>QUEUE: updates queue order
   QUEUE-->>BACKEND_SVC: fetches next item in queue
```
