
-- Dados base img dafiti
-- Sku,Url,Valor,Total vendas do produto ultimo mes,Qdt Vendida
select 
    to_char(sale_date,'YYYYMM') sale_date
   ,product_medium_image
   ,current_price
   ,sku_simple
   ,age
   ,gender
   ,country_region
   ,country  
   ,case 
      when size_is_numeric=1 
      then avg(size::bigint) over (partition by sku_config order by sale_date rows unbounded preceding)::varchar
      else size
    end                                                                                                     as size_avg 
   ,sum(current_price) over (partition by sku_simple,to_char(sale_date,'YYYYMM') rows unbounded preceding)  as sum_val_per_month_sku
   ,count(sku_simple)  over (partition by to_char(sale_date,'YYYYMM') rows unbounded preceding)             as tt_sku_per_month
from 
(select distinct
   isi.sale_order_store_date::date sale_date
  ,isi.sale_order_store_number
  ,pc.product_small_image	
  ,pc.product_large_image
  ,pc.product_medium_image
  ,pc.current_price
  ,pc.color
  ,pc.sku_config
  ,ps.sku_simple
  ,ps.size
  ,REGEXP_COUNT(ps.size, '^[0-9]+$') size_is_numeric
  ,datediff(year,dc.birthday::date,current_date) age	
  ,dc.gender
  ,da.country_region
  ,da.country
from  business_layer.dim_product_simple ps 
inner join business_layer.dim_product_config pc
        on ps.fk_product_config = pc.id_product_config
       and ps.fk_company = pc.fk_company
 left join integration_layer.int_sale_item isi
        on isi.sku_simple_store = ps.sku_simple
       and isi.fk_company_store = ps.fk_company
 left join business_layer.dim_customer dc 
        on isi.fk_customer = dc.pk_customer
       and isi.fk_company_store=dc.fk_company
 left join business_layer.dim_address da 
        on isi.fk_address_shipping = da.pk_address
       and isi.fk_company_store=da.fk_company
where 1=1
and isi.is_invalid=0
and pc.sku_config='BE139SHF93OKG'
limit 10
);


