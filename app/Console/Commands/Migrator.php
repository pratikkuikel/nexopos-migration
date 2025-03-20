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
        dd('pc');
        $this->query->get();
    }

    public function handleProducts()
    {
        dd('pr');
        $this->query->get();
    }

    public function init($table)
    {
        $this->query = DB::table($table);
    }
}
