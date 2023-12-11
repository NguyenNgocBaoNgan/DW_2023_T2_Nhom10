package com.example.weather;

import java.sql.SQLException;

public class Run {
    public static void main(String[] args) throws SQLException {

        Crawler crawler = new Crawler();
        crawler.startCrawl();
        Extract extract = new Extract();
        extract.extract();
        Transform trans = new Transform();
        trans.startTransform();
        LoadStagging_Warehouse store = new LoadStagging_Warehouse();
        store.startLoading();
        WarehouseToAggrigate warehouseToAggrigate = new WarehouseToAggrigate();
        warehouseToAggrigate.whToAggrigate();
        AggrigateToDataMart aggrigateToDataMart = new AggrigateToDataMart();
        aggrigateToDataMart.aggrigateToMart();

    }
}
