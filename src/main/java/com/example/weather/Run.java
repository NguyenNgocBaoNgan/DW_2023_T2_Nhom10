package com.example.weather;

import java.net.InetAddress;
import java.sql.SQLException;

public class Run {
    public static void main(String[] args) throws Exception {

        Crawler crawler = new Crawler();
        crawler.startCrawl();
        Extract extract = new Extract();
        extract.extract();
        Transform trans = new Transform();
        trans.startTransform();
        LoadStagging_Warehouse loadToWarehouse = new LoadStagging_Warehouse();
        loadToWarehouse.startLoading();
        WarehouseToAggregate warehouseToAggregate = new WarehouseToAggregate();
        warehouseToAggregate.whToAggregate();
        AggregateToDataMart aggregateToDataMart = new AggregateToDataMart();
        aggregateToDataMart.aggregateToMart();

    }
}
