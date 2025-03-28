<?php

namespace App\Console\Commands;

use App\TableEnum;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class Migrator extends Command
{
    protected $signature = 'hey';

    // protected $table = TableEnum::ProductsCategories;
    // protected $table = TableEnum::Products;
    // protected $table = TableEnum::UnitsGroups;
    // protected $table = TableEnum::Units;
    // protected $table = TableEnum::ProductsUnitQuantities;
    // protected $table = TableEnum::ProductsUnitQuantitiesInventory;
    // protected $table = TableEnum::Customers;
    // protected $table = TableEnum::Orders;
    protected $table = TableEnum::OrdersProducts;

    protected $query;

    protected $newDbName = 'tenant9';

    /* 5151 to sewasanitary */

    public function handle()
    {
        if ($this->table == TableEnum::ProductsUnitQuantitiesInventory) {

            $this->init('nexopos_' . $this->table->value);
        } else {
            $this->init($this->table->value);
        }

        match ($this->table) {
            TableEnum::ProductsCategories => $this->handleProductCategories(),
            TableEnum::Products => $this->handleProducts(),
            TableEnum::UnitsGroups => $this->handleUnitGroups(),
            TableEnum::Units => $this->handleUnits(),
            TableEnum::ProductsUnitQuantities => $this->handleProductUnits(),
            TableEnum::ProductsUnitQuantitiesInventory => $this->handleInventory(),
            TableEnum::Customers => $this->handleCustomers(),
            TableEnum::Orders => $this->handleOrders(),
            TableEnum::OrdersProducts => $this->handleOrderProducts(),
        };
    }

    public function handleProductCategories()
    {
        /* 5151 db */
        $cats = $this->query->get();

        $preparedCats = [];

        $cats->each(function ($item) use (&$preparedCats) {
            $preparedCats[] = [
                'id' => $item->id,
                'name' => $item->name
            ];
        });

        $this->changedb();

        /* New sewasanitary db */

        /* prune existing categories */
        $this->query->delete();

        /* Copy categories */
        $this->query->insert($preparedCats);

        $this->info('Categories copied !');
    }

    public function handleProducts()
    {
        /* 5151 db */
        $products = $this->query->get();

        $data = [];

        $products->each(function ($item) use (&$data) {
            $data[] = [
                'id' => $item->id,
                'name' => $item->name,
                'category_id' => $item->category_id,
                'visible' => true
            ];
        });

        $collection = collect($data);

        $chunks = $collection->chunk(100);

        $chunks->toArray();

        $this->changedb();

        /* New sewasanitary db */

        /* prune existing products */
        $this->query->delete();

        /* Copy products */
        foreach ($chunks as $chunk) {
            $this->query->insert($chunk->toArray());
        }

        $this->info('Products copied !');
    }

    public function handleUnitGroups()
    {
        /* 5151 db */
        $groups = $this->query->get();

        $preparedGroups = [];

        $groups->each(function ($item) use (&$preparedGroups) {
            $preparedGroups[] = [
                'id' => $item->id,
                'name' => $item->name
            ];
        });

        $this->changedb();

        /* New sewasanitary db */

        /* prune existing groups */
        $this->query->delete();

        /* Copy groups */
        $this->query->insert($preparedGroups);

        $this->info('Unit groups copied !');
    }

    public function handleUnits()
    {
        /* 5151 db */
        $units = $this->query->get();

        $preparedUnits = [];

        $units->each(function ($item) use (&$preparedUnits) {
            $preparedUnits[] = [
                'id' => $item->id,
                'name' => $item->name,
                'unit_group_id' => $item->group_id
            ];
        });

        $this->changedb();

        /* New sewasanitary db */

        /* prune existing units */
        $this->query->delete();

        /* Copy units */
        $this->query->insert($preparedUnits);

        $this->info('Units copied !');
    }

    public function handleProductUnits()
    {
        /* 5151 db */
        $products = $this->query->get();

        $data = [];

        /* gets  */
        /*needs product_id, unit_id, price conversion_factor, is_base, */
        $products->each(function ($item) use (&$data) {
            $data[] = [
                'id' => $item->id,
                'product_id' => $item->product_id,
                'unit_id' => $item->unit_id,
                'conversion_factor' => 1,
                'price' => $item->sale_price * 100, //convert to paisa while storing
                'is_base' => true,
            ];
        });

        $collection = collect($data);

        $chunks = $collection->chunk(100);

        $chunks->toArray();

        $this->changedb();

        /* New sewasanitary db */

        /* prune existing product units */
        $this->query->delete();

        /* Copy products */
        foreach ($chunks as $chunk) {
            $this->query->insert($chunk->toArray());
        }

        $this->info('Product units copied !');
    }

    public function handleInventory()
    {
        /* 5151 db */
        $products = $this->query->get();

        $data = [];

        $products->each(function ($item) use (&$data) {
            $data[] = [
                'id' => $item->id,
                'product_id' => $item->product_id,
                'current_stock' => $item->quantity,
            ];
        });

        $collection = collect($data);

        $chunks = $collection->chunk(100);

        $chunks->toArray();

        $this->changedb();

        /* New sewasanitary db */

        /* prune existing product units */
        $this->query->delete();

        /* Copy products */
        foreach ($chunks as $chunk) {
            $this->query->insert($chunk->toArray());
        }

        $this->info('Product inventory copied !');
    }

    public function handleCustomers()
    {
        /* 5151 db */
        $customers = $this->query->get();

        $data = [];

        $customers->each(function ($item) use (&$data) {
            $data[] = [
                'id' => $item->id,
                'name' => $item->name,
                'phone' => $item->phone,
            ];
        });

        $collection = collect($data);

        $chunks = $collection->chunk(100);

        $chunks->toArray();

        $this->changedb();

        /* New sewasanitary db */

        /* prune existing customers */
        $this->query->delete();

        /* Copy customers */
        foreach ($chunks as $chunk) {
            $this->query->insert($chunk->toArray());
        }

        $this->info('Customers copied !');
    }

    public function handleOrders()
    {
        /* 5151 db */
        $orders = $this->query->get();

        $data = [];

        $orders->each(function ($item) use (&$data) {
            $data[] = [
                'id' => $item->id,
                'invoice_number' => $item->code,
                'customer_id' => $item->customer_id,
                'amount' => $item->total * 100,
                'discount' => $item->discount * 100,
                'status' => 'confirmed',
                'notes' => $item->note,
            ];
        });

        $collection = collect($data);

        $chunks = $collection->chunk(100);

        $chunks->toArray();

        $this->changedb();

        /* New sewasanitary db */

        /* prune existing customers */
        $this->query->delete();

        /* Copy customers */
        foreach ($chunks as $chunk) {
            $this->query->insert($chunk->toArray());
        }

        $this->info('Orders / sales copied !');
    }

    public function handleOrderProducts()
    {
        /* 5151 db */
        $orderProducts = $this->query->get();

        $data = [];

        /* sale_id, product_id, quantity, amount */
        $orderProducts->each(function ($item) use (&$data) {
            $data[] = [
                'id' => $item->id,
                'sale_id' => $item->order_id,
                'product_id' => $item->product_id,
                'quantity' => $item->quantity,
                'amount' => $item->unit_price * 100,
            ];
        });

        $collection = collect($data);

        $chunks = $collection->chunk(100);

        $chunks->toArray();

        $this->changedb();

        /* New sewasanitary db */

        /* prune existing sales items */
        $this->query->delete();

        /* Copy order_products to sales_items */
        foreach ($chunks as $chunk) {
            $this->query->insert($chunk->toArray());
        }

        $this->info('Order items copied !');
    }

    public function changedb()
    {
        DB::purge('mysql');

        config(['database.connections.mysql.database' => $this->newDbName]);

        DB::reconnect('mysql');

        $newTablePrefix = 'pos_';

        $newTable = match ($this->table) {
            TableEnum::ProductsCategories =>  $newTablePrefix . 'product_categories',
            TableEnum::Products =>  $newTablePrefix . 'products',
            TableEnum::UnitsGroups =>  $newTablePrefix . 'unit_groups',
            TableEnum::Units =>  $newTablePrefix . 'units',
            TableEnum::ProductsUnitQuantities =>  $newTablePrefix . 'product_units',
            TableEnum::ProductsUnitQuantitiesInventory =>  $newTablePrefix . 'inventories',
            TableEnum::Customers =>  $newTablePrefix . 'customers',
            TableEnum::Orders =>  $newTablePrefix . 'sales',
            TableEnum::OrdersProducts =>  $newTablePrefix . 'sales_items',
        };

        $this->init($newTable);
    }

    public function init($table)
    {
        $this->query = DB::table($table);
    }
}
