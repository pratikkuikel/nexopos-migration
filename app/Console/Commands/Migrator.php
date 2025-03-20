<?php

namespace App\Console\Commands;

use App\TableEnum;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class Migrator extends Command
{
    protected $signature = 'hey';

    protected $description = 'Command description';

    protected $table = TableEnum::ProductsCategories;

    protected $query;

    protected $newDbName = 'tenantet';

    public function handle()
    {
        $this->init($this->table->value);

        match ($this->table) {
            TableEnum::ProductsCategories => $this->handleProductCategories(),
            TableEnum::Products => $this->handleProducts(),
        };
    }

    public function handleProductCategories()
    {
        $cats = $this->query->get();

        $preparedCats = [];

        $cats->each(function ($item) use (&$preparedCats) {
            $preparedCats[] = ['name' => $item->name];
        });

        $this->changedb();

        $this->query->get();

        // prune categories table of another db and populate it again using createMany

        /* conversion */
    }

    public function changedb()
    {
        DB::purge('mysql');

        config(['database.connections.mysql.database' => $this->newDbName]);

        DB::reconnect('mysql');

        $newTablePrefix = 'pos_';

        $newTable = match ($this->table) {
            TableEnum::ProductsCategories =>  $newTablePrefix . 'product_categories',
        };

        $this->init($newTable);
    }

    public function init($table)
    {
        $this->query = DB::table($table);
    }
}
