<?php

namespace App\Console\Commands;

use App\TableEnum;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class Migrator extends Command
{
    protected $signature = 'hey';

    protected $table = null;

    protected $query;

    protected $from = '5151';

    protected $newDbName = 'tenant7';

    /* 5151 to sewasanitary */

    public function handle()
    {
        $tables = [
            TableEnum::ProductsCategories,
            TableEnum::Products,
            TableEnum::UnitsGroups,
            TableEnum::Units,
            TableEnum::ProductsUnitQuantities,
            TableEnum::ProductsUnitQuantitiesInventory,
            TableEnum::Customers,
            TableEnum::Orders,
            TableEnum::OrdersProducts,
            TableEnum::Providers,
            TableEnum::Procurements,
            TableEnum::ProcurementsProducts,
        ];

        foreach ($tables as $table) {
            $this->table = $table;

            $this->processData();

            $this->resetdb();
        }
    }

    public function processData()
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
            TableEnum::Providers => $this->handleProviders(),
            TableEnum::Procurements => $this->handleProcurements(),
            TableEnum::ProcurementsProducts => $this->handleProcurementProducts(),
        };
    }

    public function handleProviders()
    {
        /* 5151 db */
        $purchases = $this->query->get();

        $data = [];

        $purchases->each(function ($item) use (&$data) {

            $total_purchased = DB::table('nexopos_procurements')
                ->where('provider_id', $item->id)
                ->sum('cost');

            $data[] = [
                'id' => $item->id,
                'name' => $item->name,
                'phone' => $item->phone,
                'total_purchased' => intval($total_purchased) * 100,
                'total_paid' => intval($item->amount_paid) * 100,
                'total_due' => intval($item->amount_due) * 100,
                'created_at' => $item->created_at,
                'updated_at' => $item->updated_at
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

        $this->createSupplierOpeningBalance($data);

        $this->info('Providers / suppliers copied !');
    }


    public function createSupplierOpeningBalance($data)
    {

        foreach (DB::table('pos_suppliers')->get() as $supplier) {
            DB::table('pos_ledgers')->insert([
                'ledgerable_type' => 'App\Models\Tenant\Pos\Supplier',
                'ledgerable_id' => $supplier->id,
                'type' => 'opening-balance',
                'amount' => $supplier->total_due,
                'balance' => $supplier->total_due,
                'created_at' => now(),
                'updated_at' => now()
            ]);
        }
    }

    public function handleProcurements()
    {
        /* 5151 db */
        $purchases = $this->query->get();

        $data = [];

        $purchases->each(function ($item) use (&$data) {
            $data[] = [
                'id' => $item->id,
                'notes' => $item->name,
                'supplier_id' => $item->provider_id,
                'purchase_date' => $item->invoice_date ?? now(),
                'invoice_number' => $item->invoice_reference,
                'total_amount' => intval($item->cost) * 100,
                'created_at' => $item->created_at,
                'updated_at' => $item->updated_at
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

        $this->info('Purchases copied !');
    }

    public function handleProcurementProducts()
    {
        /* 5151 db */
        $products = $this->query->get();

        $data = [];

        $products->each(function ($item) use (&$data) {
            $data[] = [
                'id' => $item->id,
                'purchase_id' => $item->procurement_id,
                'product_id' => $item->product_id,
                'quantity' => $item->quantity,
                'amount' => intval($item->purchase_price) * 100,
                'created_at' => $item->created_at,
                'updated_at' => $item->updated_at
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

        $this->info('Purchase items copied !');
    }


    public function handleProductCategories()
    {
        /* 5151 db */
        $cats = $this->query->get();

        $preparedCats = [];

        $cats->each(function ($item) use (&$preparedCats) {
            $preparedCats[] = [
                'id' => $item->id,
                'name' => ucwords($item->name),
                'created_at' => $item->created_at,
                'updated_at' => $item->updated_at
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
                'visible' => true,
                'created_at' => $item->created_at,
                'updated_at' => $item->updated_at
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
                'name' => ucfirst($item->name),
                'created_at' => $item->created_at,
                'updated_at' => $item->updated_at
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
                'name' => ucfirst($item->name),
                'unit_group_id' => $item->group_id,
                'created_at' => $item->created_at,
                'updated_at' => $item->updated_at
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
                'created_at' => $item->created_at,
                'updated_at' => $item->updated_at
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
                'created_at' => $item->created_at,
                'updated_at' => $item->updated_at
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

            $total_sold = DB::table('nexopos_orders')
                ->where('customer_id', $item->id)
                ->sum('total');

            $data[] = [
                'id' => $item->id,
                'name' => ucwords($item->name),
                'phone' => $item->phone,
                'total_sold' => intval(($total_sold * 100)),
                'total_due' => intval(($item->owed_amount * 100)),
                'total_paid' => intval(($total_sold - $item->owed_amount) * 100),
                'created_at' => $item->created_at,
                'updated_at' => $item->updated_at
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

        // Create ledger entry opening balance for due amount

        $this->createCustomerOpeningBalance($data);

        $this->info('Customers copied !');
    }

    public function createCustomerOpeningBalance($data)
    {
        DB::table('pos_ledgers')->delete();

        foreach (DB::table('pos_customers')->get() as $customer) {
            DB::table('pos_ledgers')->insert([
                'ledgerable_type' => 'App\Models\Tenant\Pos\Customer',
                'ledgerable_id' => $customer->id,
                'type' => 'opening-balance',
                'amount' => $customer->total_due,
                'balance' => $customer->total_due,
                'created_at' => now(),
                'updated_at' => now()
            ]);
        }
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
                'created_at' => $item->created_at,
                'updated_at' => $item->updated_at
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
                'created_at' => $item->created_at,
                'updated_at' => $item->updated_at
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

    public function resetdb()
    {
        DB::purge('mysql');

        config(['database.connections.mysql.database' => $this->from]);

        DB::reconnect('mysql');
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
            TableEnum::Providers => $newTablePrefix . 'suppliers',
            TableEnum::Procurements =>  $newTablePrefix . 'purchases',
            TableEnum::ProcurementsProducts =>  $newTablePrefix . 'purchase_items',
        };

        $this->init($newTable);
    }

    public function init($table)
    {
        $this->query = DB::table($table);
    }
}
