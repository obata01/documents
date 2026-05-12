
## 全体アーキテクチャ

```mermaid
flowchart LR
    %% ===== ソース =====
    subgraph SRC["Source Systems"]
        direction LR
        SRC1["業務DB / RDB"]
        SRC2["SaaS / 業務アプリ"]
        SRC3["ファイル / ログ"]
        SRC4["イベント / IoT / CDC"]
        SRC5["外部データ / 外部カタログ"]
    end

    %% ===== 1. Collecting Layer =====
    subgraph COL["1. Collecting Layer (取り込み)"]
        direction LR
        COL1["Lakeflow Connect<br/>(SaaS / DB 連携)"]
        COL2["Auto Loader<br/>(クラウドストレージ新着)"]
        COL3["Structured Streaming<br/>(Kafka / CDC / IoT)"]
        COL4["Lakehouse Federation<br/>(論理接続 / Catalog Federation)"]
    end

    %% ===== 2. Processing Layer =====
    subgraph PRC["2. Processing Layer (変換・品質)"]
        direction TB
        PRC_ENG["Apache Spark + Photon / Jobs"]
        PRC_PIPE["Lakeflow Spark Declarative Pipelines<br/>(Streaming Tables / Materialized Views)"]
        PRC_EXP["Data Quality Expectations<br/>(retain / drop / fail)"]
        PRC_QUAR["Quarantine Flow<br/>(品質違反レコード隔離)"]
        PRC_ENG --> PRC_PIPE
        PRC_PIPE --> PRC_EXP
        PRC_EXP -.違反.-> PRC_QUAR
    end

    %% ===== 3. Storage Layer (Medallion) =====
    subgraph STO["3. Storage Layer (Cloud Object Storage + Delta / Iceberg)"]
        direction LR
        BRONZE["Bronze / Raw<br/>忠実保全・再処理起点"]
        SILVER["Silver / Staging<br/>標準化・品質保証・共通再利用"]
        GOLD["Gold<br/>業務向けデータ製品<br/>(ドメイン別 dimensional model)"]
        QZ["Quarantine Zone<br/>再処理待ち"]
        TZ["Temporary Zone<br/>中間計算"]
        BRONZE --> SILVER --> GOLD
    end

    %% ===== 4. Access Layer =====
    subgraph ACC["4. Access Layer (Governed Self-Service)"]
        direction LR
        ACC1["Databricks SQL<br/>/ SQL Warehouses"]
        ACC2["AI/BI Dashboards<br/>/ Genie Spaces"]
        ACC3["Business Semantics<br/>(Metrics / Semantic Views)"]
        ACC4["External BI<br/>(Tableau / Power BI etc.)"]
        ACC5["Delta Sharing<br/>(外部共有)"]
        ACC6["AI / ML / Apps<br/>/ Reverse ETL"]
    end

    %% ===== 横断レイヤー =====
    subgraph GOV["Governance & Metadata — Unity Catalog"]
        direction LR
        GOV1["Metastore / Catalog / Schema / Table"]
        GOV2["Privileges / MANAGE"]
        GOV3["Governed Tags / ABAC"]
        GOV4["Row Filters / Column Masks"]
        GOV5["Lineage (列レベル)"]
    end

    subgraph OBS["Observability & SLO — System Tables"]
        direction LR
        OBS1["Query History"]
        OBS2["SQL Warehouse Monitor"]
        OBS3["Pipelines / Jobs Monitor"]
        OBS4["Audit Logs"]
        OBS5["Alerts / Dashboards"]
    end

    subgraph FIN["FinOps & Cost"]
        direction LR
        FIN1["Billable Usage System Table"]
        FIN2["Workload-isolated Compute<br/>(ETL / BI / DS 分離)"]
        FIN3["Chargeback / Showback Tags"]
    end

    %% ===== データ本流 =====
    SRC --> COL
    COL1 --> BRONZE
    COL2 --> BRONZE
    COL3 --> BRONZE
    COL4 -. 論理SSoT .-> ACC

    BRONZE --> PRC
    PRC --> SILVER
    SILVER --> PRC
    PRC --> GOLD
    PRC_QUAR -.-> QZ
    QZ -. 再処理 .-> PRC

    GOLD --> ACC1
    GOLD --> ACC2
    GOLD --> ACC3
    GOLD --> ACC4
    GOLD --> ACC5
    GOLD --> ACC6

    %% ===== 横断連携 (破線) =====
    COL -.メタデータ/権限.-> GOV
    PRC -.メタデータ/権限.-> GOV
    STO -.メタデータ/権限.-> GOV
    ACC -.メタデータ/権限.-> GOV

    COL -.実行ログ.-> OBS
    PRC -.実行ログ.-> OBS
    ACC -.クエリ/BI.-> OBS

    PRC -.コスト.-> FIN
    ACC -.コスト.-> FIN

    %% ===== スタイル =====
    classDef srcBox fill:#eef4ff,stroke:#6b8fd8,color:#1a2a4a
    classDef colBox fill:#e8f5e9,stroke:#5ca36a,color:#1b3d24
    classDef prcBox fill:#fff4e0,stroke:#d39a3a,color:#4a3512
    classDef bronzeBox fill:#f3d9b1,stroke:#a67848,color:#3d2a12
    classDef silverBox fill:#e0e0e0,stroke:#808080,color:#202020
    classDef goldBox fill:#ffe89a,stroke:#c9a227,color:#4a3a0a
    classDef quarBox fill:#fde0e0,stroke:#c94f4f,color:#4a1212
    classDef tempBox fill:#ececec,stroke:#999999,color:#333333
    classDef accBox fill:#e6e6fa,stroke:#7a6fc9,color:#20204a
    classDef govBox fill:#e0f2f1,stroke:#4a9b95,color:#123d3a
    classDef obsBox fill:#f1e8fb,stroke:#8a63c2,color:#2e1a4a
    classDef finBox fill:#fff0f5,stroke:#c96faa,color:#4a1a36

    class SRC1,SRC2,SRC3,SRC4,SRC5 srcBox
    class COL1,COL2,COL3,COL4 colBox
    class PRC_ENG,PRC_PIPE,PRC_EXP prcBox
    class PRC_QUAR quarBox
    class BRONZE bronzeBox
    class SILVER silverBox
    class GOLD goldBox
    class QZ quarBox
    class TZ tempBox
    class ACC1,ACC2,ACC3,ACC4,ACC5,ACC6 accBox
    class GOV1,GOV2,GOV3,GOV4,GOV5 govBox
    class OBS1,OBS2,OBS3,OBS4,OBS5 obsBox
    class FIN1,FIN2,FIN3 finBox
```

### 凡例

- **実線矢印**: データ本流 (Source → Bronze → Silver → Gold → 消費)
- **破線矢印**: メタデータ / 監視 / ガバナンス / コスト連携
- **金色ボックス**: Gold / プレゼンテーション層 (業務提供データ製品)
- **銀色ボックス**: Silver / 共通再利用層
- **茶色ボックス**: Bronze / 忠実保全層
- **赤系ボックス**: Quarantine (品質違反の隔離・再処理待ち)
- **灰色ボックス**: Temporary (永続プロダクトではない一時領域)
- **紫破線 (Federation)**: 物理移送せず外部ソースを論理接続する補助経路 (ロジカルSSoT)

### 設計思想の要点

- **Medallion (Bronze / Silver / Gold)** を品質段階の責務分離として中核に据える
- **Unity Catalog** を横断ガバナンス面とし、workspace ではなく metastore 単位で統治する
- **System Tables** を観測・監査・FinOps の共通基盤に使う
- **Governed Self-Service**: ユーザは Gold / Business Semantics を入口にし、Raw / Silver を直接触らせない
- **Federation** は物理SSoTの補助であり、最終的なデータ製品は Medallion で育てる
