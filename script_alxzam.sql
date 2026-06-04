-- ========================================
-- SCRIPT DE SINCRONIZACIÓN (Generado: 6/4/2026 6:02:09 PM)
-- ========================================


-- === VISTAS ===
-- Recreando vista: fza_caja_depositos_view
DROP VIEW IF EXISTS `fza_caja_depositos_view`;

CREATE ALGORITHM=UNDEFINED  VIEW `fza_caja_depositos_view` AS select 'ALTA' AS `ROL_EN_OPERACION`,`d`.`ID_DEPOSITO_DEP` AS `ID_DEPOSITO_DEP`,`d`.`CODIGO_EMP_DEP` AS `CODIGO_EMPRESA_OP`,`d`.`CODIGO_ALM_DEP` AS `CODIGO_ALMACEN_OP`,`d`.`CODIGO_CAJA_DEP` AS `CODIGO_CAJA_OP`,`d`.`NUMERO_OPERACION_DEP` AS `NUMERO_OPERACION_OP`,`d`.`CODIGO_CLI_DEP` AS `CODIGO_CLI_DEP`,`d`.`CODIGO_ART_DEP` AS `CODIGO_ART_DEP`,`d`.`CODIGO_UNIDAD_DEP` AS `CODIGO_UNIDAD_DEP`,`d`.`CODIGO_ALM_DEP` AS `CODIGO_ALM_DEP`,`d`.`CANTIDAD_PENDIENTE_DEP` AS `CANTIDAD_PENDIENTE_DEP`,`d`.`PRECIO_VENTA_DEP` AS `PRECIO_VENTA_DEP`,`d`.`PORCENTAJE_IVA_DEP` AS `PORCENTAJE_IVA_DEP`,`d`.`IMPORTE_ANTICIPO_DEP` AS `IMPORTE_ANTICIPO_DEP`,`d`.`PRECIO_VENTA_DEP` - `d`.`IMPORTE_ANTICIPO_DEP` AS `IMPORTE_PENDIENTE_DEP`,`d`.`ESTADO_DEP` AS `ESTADO_DEP`,`d`.`FECHA_CREACION_DEP` AS `FECHA_CREACION_DEP`,`d`.`FECHA_ENTREGA_DEP` AS `FECHA_ENTREGA_DEP`,`d`.`EMPRESA_CANCEL_DEP` AS `EMPRESA_CANCEL_DEP`,`d`.`ALMACEN_CANCEL_DEP` AS `ALMACEN_CANCEL_DEP`,`d`.`CAJA_CANCEL_DEP` AS `CAJA_CANCEL_DEP`,`d`.`NUMERO_OPERACION_CANCEL_DEP` AS `NUMERO_OPERACION_CANCEL_DEP` from `fza_depositos_cliente` `d` where `d`.`NUMERO_OPERACION_DEP` is not null and `d`.`NUMERO_OPERACION_DEP` <> '' union all select 'CANCELACION' AS `ROL_EN_OPERACION`,`d`.`ID_DEPOSITO_DEP` AS `ID_DEPOSITO_DEP`,`d`.`EMPRESA_CANCEL_DEP` AS `CODIGO_EMPRESA_OP`,`d`.`ALMACEN_CANCEL_DEP` AS `CODIGO_ALMACEN_OP`,`d`.`CAJA_CANCEL_DEP` AS `CODIGO_CAJA_OP`,`d`.`NUMERO_OPERACION_CANCEL_DEP` AS `NUMERO_OPERACION_OP`,`d`.`CODIGO_CLI_DEP` AS `CODIGO_CLI_DEP`,`d`.`CODIGO_ART_DEP` AS `CODIGO_ART_DEP`,`d`.`CODIGO_UNIDAD_DEP` AS `CODIGO_UNIDAD_DEP`,`d`.`CODIGO_ALM_DEP` AS `CODIGO_ALM_DEP`,`d`.`CANTIDAD_PENDIENTE_DEP` AS `CANTIDAD_PENDIENTE_DEP`,`d`.`PRECIO_VENTA_DEP` AS `PRECIO_VENTA_DEP`,`d`.`PORCENTAJE_IVA_DEP` AS `PORCENTAJE_IVA_DEP`,`d`.`IMPORTE_ANTICIPO_DEP` AS `IMPORTE_ANTICIPO_DEP`,`d`.`PRECIO_VENTA_DEP` - `d`.`IMPORTE_ANTICIPO_DEP` AS `IMPORTE_PENDIENTE_DEP`,`d`.`ESTADO_DEP` AS `ESTADO_DEP`,`d`.`FECHA_CREACION_DEP` AS `FECHA_CREACION_DEP`,`d`.`FECHA_ENTREGA_DEP` AS `FECHA_ENTREGA_DEP`,`d`.`EMPRESA_CANCEL_DEP` AS `EMPRESA_CANCEL_DEP`,`d`.`ALMACEN_CANCEL_DEP` AS `ALMACEN_CANCEL_DEP`,`d`.`CAJA_CANCEL_DEP` AS `CAJA_CANCEL_DEP`,`d`.`NUMERO_OPERACION_CANCEL_DEP` AS `NUMERO_OPERACION_CANCEL_DEP` from `fza_depositos_cliente` `d` where `d`.`NUMERO_OPERACION_CANCEL_DEP` is not null and `d`.`NUMERO_OPERACION_CANCEL_DEP` <> '' union all select case `o`.`TIPO_OPERACION_OPCAJA` when 'DE' then 'COBRO_INICIAL' when 'CB' then 'COBRO_PARCIAL' end AS `ROL_EN_OPERACION`,`d`.`ID_DEPOSITO_DEP` AS `ID_DEPOSITO_DEP`,`o`.`CODIGO_EMP_OPCAJA` AS `CODIGO_EMPRESA_OP`,`o`.`CODIGO_ALM_OPCAJA` AS `CODIGO_ALMACEN_OP`,`o`.`CODIGO_CAJA_OPCAJA` AS `CODIGO_CAJA_OP`,`o`.`NUMERO_OPERACION_OPCAJA` AS `NUMERO_OPERACION_OP`,`d`.`CODIGO_CLI_DEP` AS `CODIGO_CLI_DEP`,`d`.`CODIGO_ART_DEP` AS `CODIGO_ART_DEP`,`d`.`CODIGO_UNIDAD_DEP` AS `CODIGO_UNIDAD_DEP`,`d`.`CODIGO_ALM_DEP` AS `CODIGO_ALM_DEP`,`d`.`CANTIDAD_PENDIENTE_DEP` AS `CANTIDAD_PENDIENTE_DEP`,`d`.`PRECIO_VENTA_DEP` AS `PRECIO_VENTA_DEP`,`d`.`PORCENTAJE_IVA_DEP` AS `PORCENTAJE_IVA_DEP`,`d`.`IMPORTE_ANTICIPO_DEP` AS `IMPORTE_ANTICIPO_DEP`,`d`.`PRECIO_VENTA_DEP` - `d`.`IMPORTE_ANTICIPO_DEP` AS `IMPORTE_PENDIENTE_DEP`,`d`.`ESTADO_DEP` AS `ESTADO_DEP`,`d`.`FECHA_CREACION_DEP` AS `FECHA_CREACION_DEP`,`d`.`FECHA_ENTREGA_DEP` AS `FECHA_ENTREGA_DEP`,`d`.`EMPRESA_CANCEL_DEP` AS `EMPRESA_CANCEL_DEP`,`d`.`ALMACEN_CANCEL_DEP` AS `ALMACEN_CANCEL_DEP`,`d`.`CAJA_CANCEL_DEP` AS `CAJA_CANCEL_DEP`,`d`.`NUMERO_OPERACION_CANCEL_DEP` AS `NUMERO_OPERACION_CANCEL_DEP` from (`fza_caja_operaciones` `o` join `fza_depositos_cliente` `d` on(`d`.`ID_DEPOSITO_DEP` = `o`.`ID_DEPOSITO_OPCAJA`)) where `o`.`TIPO_OPERACION_OPCAJA` in ('CB','DE') and `o`.`ID_DEPOSITO_OPCAJA` is not null;

-- Recreando vista: vi_albaranes
DROP VIEW IF EXISTS `vi_albaranes`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_albaranes` AS select `fza_albaranes`.`NUMERO_ALB` AS `NUMERO_ALB`,`fza_albaranes`.`SERIE_ALB` AS `SERIE_ALB`,`fza_albaranes`.`FECHA_ALB` AS `FECHA_ALB`,`fza_albaranes`.`ESCONSOLIDADO_ALB` AS `ESCONSOLIDADO_ALB`,`fza_albaranes`.`ESTADO_ALB` AS `ESTADO_ALB`,`fza_albaranes`.`NUMERO_PED_ALB` AS `NUMERO_PED_ALB`,`fza_albaranes`.`SERIE_PED_ALB` AS `SERIE_PED_ALB`,`fza_albaranes`.`NUMERO_FAC_ALB` AS `NUMERO_FAC_ALB`,`fza_albaranes`.`SERIE_FAC_ALB` AS `SERIE_FAC_ALB`,`fza_albaranes`.`CODIGO_EMP_ALB` AS `CODIGO_EMP_ALB`,`fza_albaranes`.`RAZON_SOCIAL_EMPRESA_ALB` AS `RAZON_SOCIAL_EMPRESA_ALB`,`fza_albaranes`.`NIF_EMPRESA_ALB` AS `NIF_EMPRESA_ALB`,`fza_albaranes`.`MOVIL_EMPRESA_ALB` AS `MOVIL_EMPRESA_ALB`,`fza_albaranes`.`EMAIL_EMPRESA_ALB` AS `EMAIL_EMPRESA_ALB`,`fza_albaranes`.`DIRECCION1_EMPRESA_ALB` AS `DIRECCION1_EMPRESA_ALB`,`fza_albaranes`.`DIRECCION2_EMPRESA_ALB` AS `DIRECCION2_EMPRESA_ALB`,`fza_albaranes`.`POBLACION_EMPRESA_ALB` AS `POBLACION_EMPRESA_ALB`,`fza_albaranes`.`PROVINCIA_EMPRESA_ALB` AS `PROVINCIA_EMPRESA_ALB`,`fza_albaranes`.`CODIGO_PAI_EMPRESA_ALB` AS `CODIGO_PAI_EMPRESA_ALB`,`fza_albaranes`.`NOMBRE_PAI_EMPRESA_ALB` AS `NOMBRE_PAI_EMPRESA_ALB`,`fza_albaranes`.`CODIGO_POSTAL_EMPRESA_ALB` AS `CODIGO_POSTAL_EMPRESA_ALB`,`fza_albaranes`.`GRUPO_ZONA_IVA_EMPRESA_ALB` AS `GRUPO_ZONA_IVA_EMPRESA_ALB`,`fza_albaranes`.`CODIGO_CLI_ALB` AS `CODIGO_CLI_ALB`,`fza_albaranes`.`RAZON_SOCIAL_CLIENTE_ALB` AS `RAZON_SOCIAL_CLIENTE_ALB`,`fza_albaranes`.`NIF_CLIENTE_ALB` AS `NIF_CLIENTE_ALB`,`fza_albaranes`.`MOVIL_CLIENTE_ALB` AS `MOVIL_CLIENTE_ALB`,`fza_albaranes`.`EMAIL_CLIENTE_ALB` AS `EMAIL_CLIENTE_ALB`,`fza_albaranes`.`DIRECCION1_CLIENTE_ALB` AS `DIRECCION1_CLIENTE_ALB`,`fza_albaranes`.`DIRECCION2_CLIENTE_ALB` AS `DIRECCION2_CLIENTE_ALB`,`fza_albaranes`.`POBLACION_CLIENTE_ALB` AS `POBLACION_CLIENTE_ALB`,`fza_albaranes`.`PROVINCIA_CLIENTE_ALB` AS `PROVINCIA_CLIENTE_ALB`,`fza_albaranes`.`CODIGO_POSTAL_CLIENTE_ALB` AS `CODIGO_POSTAL_CLIENTE_ALB`,`fza_albaranes`.`CODIGO_PAI_CLIENTE_ALB` AS `CODIGO_PAI_CLIENTE_ALB`,`fza_albaranes`.`NOMBRE_PAI_CLIENTE_ALB` AS `NOMBRE_PAI_CLIENTE_ALB`,`fza_albaranes`.`NOMBRE_CLI_ENVIO_ALB` AS `NOMBRE_CLI_ENVIO_ALB`,`fza_albaranes`.`MOVIL_CLIENTE_ENVIO_ALB` AS `MOVIL_CLIENTE_ENVIO_ALB`,`fza_albaranes`.`DIRECCION1_CLIENTE_ENVIO_ALB` AS `DIRECCION1_CLIENTE_ENVIO_ALB`,`fza_albaranes`.`DIRECCION2_CLIENTE_ENVIO_ALB` AS `DIRECCION2_CLIENTE_ENVIO_ALB`,`fza_albaranes`.`POBLACION_CLIENTE_ENVIO_ALB` AS `POBLACION_CLIENTE_ENVIO_ALB`,`fza_albaranes`.`PROVINCIA_CLIENTE_ENVIO_ALB` AS `PROVINCIA_CLIENTE_ENVIO_ALB`,`fza_albaranes`.`CODIGO_POSTAL_CLIENTE_ENVIO_ALB` AS `CODIGO_POSTAL_CLIENTE_ENVIO_ALB`,`fza_albaranes`.`CODIGO_PAI_CLIENTE_ENVIO_ALB` AS `CODIGO_PAI_CLIENTE_ENVIO_ALB`,`fza_albaranes`.`NOMBRE_PAI_CLIENTE_ENVIO_ALB` AS `NOMBRE_PAI_CLIENTE_ENVIO_ALB`,`fza_albaranes`.`TRANSPORTISTA_ALB` AS `TRANSPORTISTA_ALB`,`fza_albaranes`.`CODIGO_IVA_ALB` AS `CODIGO_IVA_ALB`,`fza_albaranes`.`ESIVA_RECARGO_CLIENTE_ALB` AS `ESIVA_RECARGO_CLIENTE_ALB`,`fza_albaranes`.`ESIVA_EXENTO_CLIENTE_ALB` AS `ESIVA_EXENTO_CLIENTE_ALB`,`fza_albaranes`.`ESINTRACOMUNITARIO_CLIENTE_ALB` AS `ESINTRACOMUNITARIO_CLIENTE_ALB`,`fza_albaranes`.`TARIFA_ARTICULO_CLIENTE_ALB` AS `TARIFA_ARTICULO_CLIENTE_ALB`,`fza_albaranes`.`ESIMP_INCL_TARIFA_CLIENTE_ALB` AS `ESIMP_INCL_TARIFA_CLIENTE_ALB`,`fza_albaranes`.`PORCENTAJE_IVAN_ALB` AS `PORCENTAJE_IVAN_ALB`,`fza_albaranes`.`TOTAL_IVAN_ALB` AS `TOTAL_IVAN_ALB`,`fza_albaranes`.`PORCENTAJE_IVAR_ALB` AS `PORCENTAJE_IVAR_ALB`,`fza_albaranes`.`TOTAL_IVAR_ALB` AS `TOTAL_IVAR_ALB`,`fza_albaranes`.`PORCENTAJE_IVAS_ALB` AS `PORCENTAJE_IVAS_ALB`,`fza_albaranes`.`TOTAL_IVAS_ALB` AS `TOTAL_IVAS_ALB`,`fza_albaranes`.`PORCENTAJE_IVAE_ALB` AS `PORCENTAJE_IVAE_ALB`,`fza_albaranes`.`TOTAL_IVAE_ALB` AS `TOTAL_IVAE_ALB`,`fza_albaranes`.`TOTAL_BASES_ALB` AS `TOTAL_BASES_ALB`,`fza_albaranes`.`TOTAL_IMPUESTOS_ALB` AS `TOTAL_IMPUESTOS_ALB`,`fza_albaranes`.`TOTAL_LIQUIDO_ALB` AS `TOTAL_LIQUIDO_ALB`,`fza_albaranes`.`FORMA_PAGO_ALB` AS `FORMA_PAGO_ALB`,`fza_albaranes`.`CONTADOR_LINEAS_ALB` AS `CONTADOR_LINEAS_ALB`,`fza_albaranes`.`COMENTARIOS_ALB` AS `COMENTARIOS_ALB`,`fza_albaranes`.`OBSERVACIONES_ALB` AS `OBSERVACIONES_ALB`,`fza_albaranes`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_albaranes`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_albaranes`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_albaranes`.`USUARIO_MODIF` AS `USUARIO_MODIF` from `fza_albaranes`;

-- Recreando vista: vi_albaranes_compra
DROP VIEW IF EXISTS `vi_albaranes_compra`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_albaranes_compra` AS select `a`.`NUMERO_ALBC` AS `NUMERO_ALBC`,`a`.`SERIE_ALBC` AS `SERIE_ALBC`,`a`.`FECHA_ALBC` AS `FECHA_ALBC`,`a`.`ESTADO_ALBC` AS `ESTADO_ALBC`,`a`.`NUMERO_PED_ALBC` AS `NUMERO_PED_ALBC`,`a`.`SERIE_PED_ALBC` AS `SERIE_PED_ALBC`,`a`.`NUMERO_FAC_ALBC` AS `NUMERO_FAC_ALBC`,`a`.`SERIE_FAC_ALBC` AS `SERIE_FAC_ALBC`,`a`.`CODIGO_EMP_ALBC` AS `CODIGO_EMP_ALBC`,`a`.`RAZON_SOCIAL_EMPRESA_ALBC` AS `RAZON_SOCIAL_EMPRESA_ALBC`,`a`.`NIF_EMPRESA_ALBC` AS `NIF_EMPRESA_ALBC`,`a`.`MOVIL_EMPRESA_ALBC` AS `MOVIL_EMPRESA_ALBC`,`a`.`EMAIL_EMPRESA_ALBC` AS `EMAIL_EMPRESA_ALBC`,`a`.`DIRECCION1_EMPRESA_ALBC` AS `DIRECCION1_EMPRESA_ALBC`,`a`.`DIRECCION2_EMPRESA_ALBC` AS `DIRECCION2_EMPRESA_ALBC`,`a`.`POBLACION_EMPRESA_ALBC` AS `POBLACION_EMPRESA_ALBC`,`a`.`PROVINCIA_EMPRESA_ALBC` AS `PROVINCIA_EMPRESA_ALBC`,`a`.`CODIGO_PAI_EMPRESA_ALBC` AS `CODIGO_PAI_EMPRESA_ALBC`,`a`.`NOMBRE_PAI_EMPRESA_ALBC` AS `NOMBRE_PAI_EMPRESA_ALBC`,`a`.`CODIGO_POSTAL_EMPRESA_ALBC` AS `CODIGO_POSTAL_EMPRESA_ALBC`,`a`.`CODIGO_PRV_ALBC` AS `CODIGO_PRV_ALBC`,`a`.`RAZON_SOCIAL_PRV_ALBC` AS `RAZON_SOCIAL_PRV_ALBC`,`a`.`NIF_PRV_ALBC` AS `NIF_PRV_ALBC`,`a`.`MOVIL_PRV_ALBC` AS `MOVIL_PRV_ALBC`,`a`.`EMAIL_PRV_ALBC` AS `EMAIL_PRV_ALBC`,`a`.`DIRECCION1_PRV_ALBC` AS `DIRECCION1_PRV_ALBC`,`a`.`DIRECCION2_PRV_ALBC` AS `DIRECCION2_PRV_ALBC`,`a`.`POBLACION_PRV_ALBC` AS `POBLACION_PRV_ALBC`,`a`.`PROVINCIA_PRV_ALBC` AS `PROVINCIA_PRV_ALBC`,`a`.`CODIGO_PAI_PRV_ALBC` AS `CODIGO_PAI_PRV_ALBC`,`a`.`NOMBRE_PAI_PRV_ALBC` AS `NOMBRE_PAI_PRV_ALBC`,`a`.`CODIGO_POSTAL_PRV_ALBC` AS `CODIGO_POSTAL_PRV_ALBC`,`a`.`REF_PROVEEDOR_ALBC` AS `REF_PROVEEDOR_ALBC`,`a`.`CODIGO_ALM_ALBC` AS `CODIGO_ALM_ALBC`,`a`.`TRANSPORTISTA_ALBC` AS `TRANSPORTISTA_ALBC`,`a`.`CODIGO_IVA_ALBC` AS `CODIGO_IVA_ALBC`,`a`.`PORCENTAJE_IVAN_ALBC` AS `PORCENTAJE_IVAN_ALBC`,`a`.`TOTAL_IVAN_ALBC` AS `TOTAL_IVAN_ALBC`,`a`.`PORCENTAJE_IVAR_ALBC` AS `PORCENTAJE_IVAR_ALBC`,`a`.`TOTAL_IVAR_ALBC` AS `TOTAL_IVAR_ALBC`,`a`.`PORCENTAJE_IVAS_ALBC` AS `PORCENTAJE_IVAS_ALBC`,`a`.`TOTAL_IVAS_ALBC` AS `TOTAL_IVAS_ALBC`,`a`.`PORCENTAJE_IVAE_ALBC` AS `PORCENTAJE_IVAE_ALBC`,`a`.`TOTAL_IVAE_ALBC` AS `TOTAL_IVAE_ALBC`,`a`.`TOTAL_BASES_ALBC` AS `TOTAL_BASES_ALBC`,`a`.`TOTAL_IMPUESTOS_ALBC` AS `TOTAL_IMPUESTOS_ALBC`,`a`.`TOTAL_LIQUIDO_ALBC` AS `TOTAL_LIQUIDO_ALBC`,`a`.`FORMA_PAGO_ALBC` AS `FORMA_PAGO_ALBC`,`a`.`CONTADOR_LINEAS_ALBC` AS `CONTADOR_LINEAS_ALBC`,`a`.`COMENTARIOS_ALBC` AS `COMENTARIOS_ALBC`,`a`.`OBSERVACIONES_ALBC` AS `OBSERVACIONES_ALBC`,`a`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`a`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`a`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`a`.`USUARIO_MODIF` AS `USUARIO_MODIF`,`prv`.`NOMBRE_PRV` AS `NOMBRE_PRV_ALBC`,`emp`.`RAZON_SOCIAL_EMP` AS `RAZON_SOCIAL_EMPRESA_VIEW_ALBC` from ((`fza_albaranes_compra` `a` left join `fza_proveedores` `prv` on(`prv`.`CODIGO_PRV_PRV` = `a`.`CODIGO_PRV_ALBC`)) left join `fza_empresas` `emp` on(`emp`.`CODIGO_EMP_EMP` = `a`.`CODIGO_EMP_ALBC`));

-- Recreando vista: vi_albaranes_compra_cab_print
DROP VIEW IF EXISTS `vi_albaranes_compra_cab_print`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_albaranes_compra_cab_print` AS select `alb`.`SERIE_ALBC` AS `SERIE_ALBC`,`alb`.`NUMERO_ALBC` AS `NUMERO_ALBC`,`alb`.`FECHA_ALBC` AS `FECHA_ALBC`,`alb`.`ESTADO_ALBC` AS `ESTADO_ALBC`,`alb`.`REF_PROVEEDOR_ALBC` AS `REF_PROVEEDOR_ALBC`,`alb`.`COMENTARIOS_ALBC` AS `COMENTARIOS_ALBC`,`alb`.`OBSERVACIONES_ALBC` AS `OBSERVACIONES_ALBC`,`alb`.`CODIGO_EMP_ALBC` AS `CODIGO_EMP_ALBC`,`emp`.`RAZON_SOCIAL_EMP` AS `RAZON_SOCIAL_EMP`,`emp`.`DIRECCION1_EMP` AS `DIRECCION1_EMP`,`emp`.`CODIGO_POSTAL_EMP` AS `CODIGO_POSTAL_EMP`,`emp`.`POBLACION_EMP` AS `POBLACION_EMP`,`emp`.`PROVINCIA_EMP` AS `PROVINCIA_EMP`,`emp`.`NIF_EMP` AS `CIF_EMP`,`emp`.`MOVIL_EMP` AS `TELEFONO1_EMP`,`alb`.`CODIGO_PRV_ALBC` AS `CODIGO_PRV_ALBC`,`prv`.`RAZON_SOCIAL_PRV` AS `RAZON_SOCIAL_PRV`,`prv`.`DIRECCION1_PRV` AS `DIRECCION1_PRV`,`prv`.`CODIGO_POSTAL_PRV` AS `CODIGO_POSTAL_PRV`,`prv`.`POBLACION_PRV` AS `POBLACION_PRV`,`prv`.`PROVINCIA_PRV` AS `PROVINCIA_PRV`,`prv`.`NIF_PRV` AS `CIF_PRV`,coalesce(`prv`.`TELEFONO_PRV`,`prv`.`MOVIL_PRV`) AS `TELEFONO1_PRV`,`alb`.`CODIGO_ALM_ALBC` AS `CODIGO_ALM_ALBC`,`alm`.`NOMBRE_ALM_ALM` AS `NOMBRE_ALM_ALBC`,`alm`.`DIRECCION_ALM` AS `DIRECCION_ALM_ALBC`,`alm`.`CODIGO_POSTAL_ALM` AS `CODIGO_POSTAL_ALM_ALBC`,`alm`.`POBLACION_ALM` AS `POBLACION_ALM_ALBC`,`alm`.`PROVINCIA_ALM` AS `PROVINCIA_ALM_ALBC`,`alm`.`TELEFONO_ALM` AS `TELEFONO_ALM_ALBC`,`alm`.`EMAIL_ALM` AS `EMAIL_ALM_ALBC`,`alb`.`CODIGO_IVA_ALBC` AS `CODIGO_IVA_ALBC`,`alb`.`PORCENTAJE_IVAN_ALBC` AS `PORCENTAJE_IVAN_ALBC`,`alb`.`PORCENTAJE_IVAR_ALBC` AS `PORCENTAJE_IVAR_ALBC`,`alb`.`PORCENTAJE_IVAS_ALBC` AS `PORCENTAJE_IVAS_ALBC`,`alb`.`PORCENTAJE_IVAE_ALBC` AS `PORCENTAJE_IVAE_ALBC`,`alb`.`TOTAL_BASES_ALBC` AS `TOTAL_BASES_ALBC`,`alb`.`TOTAL_IMPUESTOS_ALBC` AS `TOTAL_IMPUESTOS_ALBC`,`alb`.`TOTAL_LIQUIDO_ALBC` AS `TOTAL_LIQUIDO_ALBC`,`alb`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`alb`.`USUARIO_ALTA` AS `USUARIO_ALTA`,(select coalesce(sum(`lin`.`CANTIDAD_ALBCLIN`),0) from `fza_albaranes_compra_lineas` `lin` where `lin`.`SERIE_ALBC_ALBCLIN` = `alb`.`SERIE_ALBC` and `lin`.`NUMERO_ALBC_ALBCLIN` = `alb`.`NUMERO_ALBC`) AS `TOTAL_UNIDADES_SES`,(select coalesce(sum(`lin`.`TOTAL_ALBCLIN`),0) from `fza_albaranes_compra_lineas` `lin` where `lin`.`SERIE_ALBC_ALBCLIN` = `alb`.`SERIE_ALBC` and `lin`.`NUMERO_ALBC_ALBCLIN` = `alb`.`NUMERO_ALBC`) AS `TOTAL_LINEAS_SES`,(select count(0) from `fza_albaranes_compra_lineas` `lin` where `lin`.`SERIE_ALBC_ALBCLIN` = `alb`.`SERIE_ALBC` and `lin`.`NUMERO_ALBC_ALBCLIN` = `alb`.`NUMERO_ALBC`) AS `NUM_LINEAS_SES` from (((`fza_albaranes_compra` `alb` left join `fza_empresas` `emp` on(`emp`.`CODIGO_EMP_EMP` = `alb`.`CODIGO_EMP_ALBC`)) left join `fza_proveedores` `prv` on(`prv`.`CODIGO_PRV_PRV` = `alb`.`CODIGO_PRV_ALBC`)) left join `fza_almacenes` `alm` on(`alm`.`CODIGO_ALM_ALM` = `alb`.`CODIGO_ALM_ALBC`));

-- Recreando vista: vi_albaranes_compra_guias_print
DROP VIEW IF EXISTS `vi_albaranes_compra_guias_print`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_albaranes_compra_guias_print` AS with pos_acd as (select `acd`.`ID_AC_ACD` AS `ID_AC`,`acd`.`ID_AV_ACD` AS `ID_AV`,`av`.`AV` AS `AV`,row_number() over ( partition by `acd`.`ID_AC_ACD` order by `acd`.`ORDEN_ACD`,`acd`.`ID_AV_ACD`) AS `POSICION` from (`fza_atributos_conjuntos_det` `acd` join `fza_atributos_valores` `av` on(`av`.`ID_AV` = `acd`.`ID_AV_ACD`)))select `ac`.`ID_AC` AS `ID_AC`,`ac`.`NOMBRE_AC` AS `NOMBRE_AC`,coalesce(`ac`.`NOMBRE_CORTO_AC`,ucase(left(`ac`.`NOMBRE_AC`,8))) AS `NOMBRE_CORTO_AC`,max(case when `p`.`POSICION` = 1 then `p`.`AV` end) AS `T01`,max(case when `p`.`POSICION` = 2 then `p`.`AV` end) AS `T02`,max(case when `p`.`POSICION` = 3 then `p`.`AV` end) AS `T03`,max(case when `p`.`POSICION` = 4 then `p`.`AV` end) AS `T04`,max(case when `p`.`POSICION` = 5 then `p`.`AV` end) AS `T05`,max(case when `p`.`POSICION` = 6 then `p`.`AV` end) AS `T06`,max(case when `p`.`POSICION` = 7 then `p`.`AV` end) AS `T07`,max(case when `p`.`POSICION` = 8 then `p`.`AV` end) AS `T08`,max(case when `p`.`POSICION` = 9 then `p`.`AV` end) AS `T09`,max(case when `p`.`POSICION` = 10 then `p`.`AV` end) AS `T10`,max(case when `p`.`POSICION` = 11 then `p`.`AV` end) AS `T11`,max(case when `p`.`POSICION` = 12 then `p`.`AV` end) AS `T12`,max(case when `p`.`POSICION` = 13 then `p`.`AV` end) AS `T13`,max(case when `p`.`POSICION` = 14 then `p`.`AV` end) AS `T14`,max(case when `p`.`POSICION` = 15 then `p`.`AV` end) AS `T15`,max(case when `p`.`POSICION` = 16 then `p`.`AV` end) AS `T16`,max(case when `p`.`POSICION` = 17 then `p`.`AV` end) AS `T17`,max(case when `p`.`POSICION` = 18 then `p`.`AV` end) AS `T18`,max(case when `p`.`POSICION` = 19 then `p`.`AV` end) AS `T19`,max(case when `p`.`POSICION` = 20 then `p`.`AV` end) AS `T20` from (`fza_atributos_conjuntos` `ac` join `pos_acd` `p` on(`p`.`ID_AC` = `ac`.`ID_AC`)) where `p`.`POSICION` <= 20 and `ac`.`ESACTIVO_AC` = 'S' group by `ac`.`ID_AC`,`ac`.`NOMBRE_AC`,`ac`.`NOMBRE_CORTO_AC`;

-- Recreando vista: vi_albaranes_compra_lin_print
DROP VIEW IF EXISTS `vi_albaranes_compra_lin_print`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_albaranes_compra_lin_print` AS with pos_acd as (select `fza_atributos_conjuntos_det`.`ID_AC_ACD` AS `ID_AC`,`fza_atributos_conjuntos_det`.`ID_AV_ACD` AS `ID_AV`,row_number() over ( partition by `fza_atributos_conjuntos_det`.`ID_AC_ACD` order by `fza_atributos_conjuntos_det`.`ORDEN_ACD`,`fza_atributos_conjuntos_det`.`ID_AV_ACD`) AS `POSICION` from `fza_atributos_conjuntos_det`), sku_talla as (select `sa`.`CODIGO_UNIDAD_SKU_SA` AS `CODIGO_UNIDAD`,`sa`.`ID_AV_SA` AS `ID_AV_TALLA` from (`fza_atributos_sku` `sa` join `fza_atributos_valores` `av` on(`av`.`ID_AV` = `sa`.`ID_AV_SA` and `av`.`ID_VA_AV` = 'TAL'))), sku_color as (select `sa`.`CODIGO_UNIDAD_SKU_SA` AS `CODIGO_UNIDAD`,`atb`.`CODIGO_ATB` AS `CODIGO_ATB_COLOR`,`atb`.`NOMBRE_ATB` AS `NOMBRE_COLOR` from ((`fza_atributos_sku` `sa` join `fza_atributos_valores` `av` on(`av`.`ID_AV` = `sa`.`ID_AV_SA` and `av`.`ID_VA_AV` = 'CO')) left join `fza_atributos_basicos` `atb` on(`atb`.`ID_ATB` = `av`.`ID_ATB_AV`)))select `l`.`SERIE_ALBC_ALBCLIN` AS `SERIE_ALBC`,`l`.`NUMERO_ALBC_ALBCLIN` AS `NUMERO_ALBC`,min(`l`.`LINEA_ALBCLIN`) AS `LINEA_ALBC`,`l`.`CODIGO_ART_ALBCLIN` AS `CODIGO_ART`,coalesce(min(`l`.`REF_PRV_ALBCLIN`),'') AS `REF_PRV`,min(`l`.`DESCRIPCION_ARTICULO_ALBCLIN`) AS `DESCRIPCION`,coalesce(min(`sc`.`NOMBRE_COLOR`),substring_index(substring_index(min(`l`.`CODIGO_UNIDAD_ALBCLIN`),'/',2),'/',-1),'') AS `COLOR_TEXTO`,coalesce(min(`sc`.`CODIGO_ATB_COLOR`),'') AS `CODIGO_ATB_COLOR`,avg(`l`.`PRECIO_COMPRA_SIVA_ARTICULO_ALBCLIN`) AS `PRECIO_COMPRA`,0 AS `PRECIO_VENTA`,`l`.`ID_AC_PIVOT_ALBCLIN` AS `ID_AC_PIVOT`,`ac`.`NOMBRE_AC` AS `NOMBRE_AC`,coalesce(`ac`.`NOMBRE_CORTO_AC`,ucase(left(`ac`.`NOMBRE_AC`,8))) AS `NOMBRE_CORTO_AC`,`l`.`CODIGO_ALMACEN_ALBCLIN` AS `CODIGO_ALM_ALBCLIN`,min(`alm`.`CODIGO_EMP_ALM`) AS `CODIGO_EMP_ALM_ALBCLIN`,min(`alm`.`NOMBRE_ALM_ALM`) AS `NOMBRE_ALM_ALBCLIN`,min(`alm`.`DIRECCION_ALM`) AS `DIRECCION_ALM_ALBCLIN`,min(`alm`.`CODIGO_POSTAL_ALM`) AS `CODIGO_POSTAL_ALM_ALBCLIN`,min(`alm`.`POBLACION_ALM`) AS `POBLACION_ALM_ALBCLIN`,min(`alm`.`PROVINCIA_ALM`) AS `PROVINCIA_ALM_ALBCLIN`,min(`alm`.`TELEFONO_ALM`) AS `TELEFONO_ALM_ALBCLIN`,min(`alm`.`EMAIL_ALM`) AS `EMAIL_ALM_ALBCLIN`,sum(`l`.`CANTIDAD_ALBCLIN`) AS `TOTAL_UNIDADES`,sum(`l`.`TOTAL_ALBCLIN`) AS `TOTAL_LINEA`,coalesce(sum(case when `p`.`POSICION` = 1 then `l`.`CANTIDAD_ALBCLIN` end),0) AS `T01`,coalesce(sum(case when `p`.`POSICION` = 2 then `l`.`CANTIDAD_ALBCLIN` end),0) AS `T02`,coalesce(sum(case when `p`.`POSICION` = 3 then `l`.`CANTIDAD_ALBCLIN` end),0) AS `T03`,coalesce(sum(case when `p`.`POSICION` = 4 then `l`.`CANTIDAD_ALBCLIN` end),0) AS `T04`,coalesce(sum(case when `p`.`POSICION` = 5 then `l`.`CANTIDAD_ALBCLIN` end),0) AS `T05`,coalesce(sum(case when `p`.`POSICION` = 6 then `l`.`CANTIDAD_ALBCLIN` end),0) AS `T06`,coalesce(sum(case when `p`.`POSICION` = 7 then `l`.`CANTIDAD_ALBCLIN` end),0) AS `T07`,coalesce(sum(case when `p`.`POSICION` = 8 then `l`.`CANTIDAD_ALBCLIN` end),0) AS `T08`,coalesce(sum(case when `p`.`POSICION` = 9 then `l`.`CANTIDAD_ALBCLIN` end),0) AS `T09`,coalesce(sum(case when `p`.`POSICION` = 10 then `l`.`CANTIDAD_ALBCLIN` end),0) AS `T10`,coalesce(sum(case when `p`.`POSICION` = 11 then `l`.`CANTIDAD_ALBCLIN` end),0) AS `T11`,coalesce(sum(case when `p`.`POSICION` = 12 then `l`.`CANTIDAD_ALBCLIN` end),0) AS `T12`,coalesce(sum(case when `p`.`POSICION` = 13 then `l`.`CANTIDAD_ALBCLIN` end),0) AS `T13`,coalesce(sum(case when `p`.`POSICION` = 14 then `l`.`CANTIDAD_ALBCLIN` end),0) AS `T14`,coalesce(sum(case when `p`.`POSICION` = 15 then `l`.`CANTIDAD_ALBCLIN` end),0) AS `T15`,coalesce(sum(case when `p`.`POSICION` = 16 then `l`.`CANTIDAD_ALBCLIN` end),0) AS `T16`,coalesce(sum(case when `p`.`POSICION` = 17 then `l`.`CANTIDAD_ALBCLIN` end),0) AS `T17`,coalesce(sum(case when `p`.`POSICION` = 18 then `l`.`CANTIDAD_ALBCLIN` end),0) AS `T18`,coalesce(sum(case when `p`.`POSICION` = 19 then `l`.`CANTIDAD_ALBCLIN` end),0) AS `T19`,coalesce(sum(case when `p`.`POSICION` = 20 then `l`.`CANTIDAD_ALBCLIN` end),0) AS `T20` from (((((`fza_albaranes_compra_lineas` `l` left join `fza_atributos_conjuntos` `ac` on(`ac`.`ID_AC` = `l`.`ID_AC_PIVOT_ALBCLIN`)) left join `sku_talla` `st` on(`st`.`CODIGO_UNIDAD` = `l`.`CODIGO_UNIDAD_ALBCLIN`)) left join `pos_acd` `p` on(`p`.`ID_AC` = `l`.`ID_AC_PIVOT_ALBCLIN` and `p`.`ID_AV` = `st`.`ID_AV_TALLA`)) left join `sku_color` `sc` on(`sc`.`CODIGO_UNIDAD` = `l`.`CODIGO_UNIDAD_ALBCLIN`)) left join `fza_almacenes` `alm` on(`alm`.`CODIGO_ALM_ALM` = `l`.`CODIGO_ALMACEN_ALBCLIN`)) group by `l`.`SERIE_ALBC_ALBCLIN`,`l`.`NUMERO_ALBC_ALBCLIN`,`l`.`CODIGO_ART_ALBCLIN`,`l`.`ID_AC_PIVOT_ALBCLIN`,`ac`.`NOMBRE_AC`,`ac`.`NOMBRE_CORTO_AC`,`sc`.`CODIGO_ATB_COLOR`,`sc`.`NOMBRE_COLOR`,substring_index(substring_index(`l`.`CODIGO_UNIDAD_ALBCLIN`,'/',2),'/',-1),`l`.`CODIGO_ALMACEN_ALBCLIN`;

-- Recreando vista: vi_albaranes_lineas
DROP VIEW IF EXISTS `vi_albaranes_lineas`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_albaranes_lineas` AS select `fza_albaranes_lineas`.`NUMERO_ALB_ALBLIN` AS `NUMERO_ALB_ALBLIN`,`fza_albaranes_lineas`.`SERIE_ALB_ALBLIN` AS `SERIE_ALB_ALBLIN`,`fza_albaranes_lineas`.`LINEA_ALBLIN` AS `LINEA_ALBLIN`,`fza_albaranes_lineas`.`NUMERO_PED_ALBLIN` AS `NUMERO_PED_ALBLIN`,`fza_albaranes_lineas`.`SERIE_PED_ALBLIN` AS `SERIE_PED_ALBLIN`,`fza_albaranes_lineas`.`LINEA_PED_ALBLIN` AS `LINEA_PED_ALBLIN`,`fza_albaranes_lineas`.`CODIGO_ART_ALBLIN` AS `CODIGO_ART_ALBLIN`,`fza_albaranes_lineas`.`CODIGO_FAM_ALBLIN` AS `CODIGO_FAM_ALBLIN`,`fza_albaranes_lineas`.`NOMBRE_FAM_ALBLIN` AS `NOMBRE_FAM_ALBLIN`,`fza_albaranes_lineas`.`DESCRIPCION_ARTICULO_ALBLIN` AS `DESCRIPCION_ARTICULO_ALBLIN`,`fza_albaranes_lineas`.`TIPO_CANTIDAD_ARTICULO_ALBLIN` AS `TIPO_CANTIDAD_ARTICULO_ALBLIN`,`fza_albaranes_lineas`.`CANTIDAD_ALBLIN` AS `CANTIDAD_ALBLIN`,`fza_albaranes_lineas`.`CODIGO_TAR_ALBLIN` AS `CODIGO_TAR_ALBLIN`,`fza_albaranes_lineas`.`ESIMP_INCL_TARIFA_ALBLIN` AS `ESIMP_INCL_TARIFA_ALBLIN`,`fza_albaranes_lineas`.`TIPO_IVA_ARTICULO_ALBLIN` AS `TIPO_IVA_ARTICULO_ALBLIN`,`fza_albaranes_lineas`.`PORCENTAJE_IVA_ALBLIN` AS `PORCENTAJE_IVA_ALBLIN`,`fza_albaranes_lineas`.`PRECIO_VENTA_SIVA_ARTICULO_ALBLIN` AS `PRECIO_VENTA_SIVA_ARTICULO_ALBLIN`,`fza_albaranes_lineas`.`PRECIO_VENTA_CIVA_ARTICULO_ALBLIN` AS `PRECIO_VENTA_CIVA_ARTICULO_ALBLIN`,`fza_albaranes_lineas`.`TOTAL_ALBLIN` AS `TOTAL_ALBLIN`,`fza_albaranes_lineas`.`CODIGO_ALMACEN_ALBLIN` AS `CODIGO_ALMACEN_ALBLIN`,`fza_albaranes_lineas`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_albaranes_lineas`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_albaranes_lineas`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_albaranes_lineas`.`USUARIO_MODIF` AS `USUARIO_MODIF` from `fza_albaranes_lineas`;

-- Recreando vista: vi_articulos
DROP VIEW IF EXISTS `vi_articulos`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_articulos` AS select `art`.`CODIGO_ART_ART` AS `CODIGO_ART_ART`,`art`.`ESACTIVO_ART` AS `ESACTIVO_ART`,`art`.`ORDEN_ART` AS `ORDEN_ART`,`art`.`DESCRIPCION_ART` AS `DESCRIPCION_ART`,`art`.`ESVARIACION_ART` AS `ESVARIACION_ART`,`art`.`ESTRAZABLE_ART` AS `ESTRAZABLE_ART`,`art`.`TIPO_ART` AS `TIPO_ART`,`art`.`TIPO_VARIACION_ART` AS `TIPO_VARIACION_ART`,`art`.`CODIGO_FAM_ART` AS `CODIGO_FAM_ART`,`fam`.`DESCRIPCION_FAM` AS `DESCRIPCION_FAM`,`fam`.`NOMBRE_FAM_FAM` AS `NOMBRE_FAM_FAM`,`art`.`TIPO_IVA_ART` AS `TIPO_IVA_ART`,`iva`.`NOMBRE_TIPO_IVA_IVATIP` AS `NOMBRE_TIPO_IVA_IVATIP`,`art`.`ESACTIVO_FIJO_ART` AS `ESACTIVO_FIJO_ART`,`art`.`TIPO_CANTIDAD_ART` AS `TIPO_CANTIDAD_ART`,`ap`.`CODIGO_PRV_AP` AS `CODIGO_PRV_AP`,`prv`.`RAZON_SOCIAL_PRV` AS `RAZON_SOCIAL_PRV`,`prv`.`NOMBRE_PRV` AS `NOMBRE_PRV`,`ap`.`REF_PROVEEDOR_AP` AS `REF_PROVEEDOR`,coalesce(`pv`.`PV`,`atemp`.`VALOR_LIBRE_ARTPROP`) AS `TEMPORADA_ART`,`art`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`art`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`art`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`art`.`USUARIO_MODIF` AS `USUARIO_MODIF` from ((((((`fza_articulos` `art` left join `fza_articulos_familias` `fam` on(`art`.`CODIGO_FAM_ART` = `fam`.`CODIGO_FAM_FAM`)) left join `fza_articulos_proveedores` `ap` on(`art`.`CODIGO_ART_ART` = `ap`.`CODIGO_ART_AP` and `ap`.`ESPROVEEDORPRINCIPAL_AP` = 'S')) left join `fza_proveedores` `prv` on(`ap`.`CODIGO_PRV_AP` = `prv`.`CODIGO_PRV_PRV`)) left join `fza_ivas_tipos` `iva` on(`art`.`TIPO_IVA_ART` = `iva`.`CODIGO_ABREVIATURA_IVA_IVATIP`)) left join `fza_articulos_propiedades` `atemp` on(`art`.`CODIGO_ART_ART` = `atemp`.`CODIGO_ART_ART` and `atemp`.`CODIGO_PROP_ARTPROP` = 'TEMPORADA')) left join `fza_propiedades_valores` `pv` on(`atemp`.`ID_PV_ARTPROP` = `pv`.`ID_PV_ARTPROP`)) order by `art`.`ORDEN_ART`;

-- Recreando vista: vi_articulos_conjuntos_slots
DROP VIEW IF EXISTS `vi_articulos_conjuntos_slots`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_articulos_conjuntos_slots` AS select `a`.`CODIGO_ART_ART` AS `CODIGO_ART_ACA`,`a`.`TIPO_VARIACION_ART` AS `ID_VAR_AC`,`va`.`ID_ATB_VA` AS `ID_VA_ACA`,coalesce(`va`.`NOMBRE_VA`,`va`.`ID_ATB_VA`) AS `NOMBRE_ATRIBUTO`,`va`.`ORDEN_VA` AS `ORDEN_ATRIBUTO`,`v`.`NOMBRE_VAR` AS `NOMBRE_VARIACION`,`aca`.`ID_AC_ACA` AS `ID_AC_ACA`,`ac`.`NOMBRE_AC` AS `NOMBRE_AC`,`ac`.`ID_VA_AC` AS `ID_VA_AC`,coalesce(`aca`.`ESGENERACION_AUTO_ACA`,'S') AS `ESGENERACION_AUTO_ACA`,`aca`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`aca`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`aca`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`aca`.`USUARIO_MODIF` AS `USUARIO_MODIF` from ((((`fza_articulos` `a` join `fza_variaciones_atributos` `va` on(`va`.`ID_VAR_VA` = `a`.`TIPO_VARIACION_ART`)) join `fza_variaciones` `v` on(`v`.`CODIGO_VAR` = `a`.`TIPO_VARIACION_ART`)) left join `fza_articulos_conjuntos_asign` `aca` on(`aca`.`CODIGO_ART_ACA` = `a`.`CODIGO_ART_ART` and `aca`.`ID_VA_ACA` = `va`.`ID_ATB_VA`)) left join `fza_atributos_conjuntos` `ac` on(`ac`.`ID_AC` = `aca`.`ID_AC_ACA`)) where `a`.`ESVARIACION_ART` = 'S' and `a`.`TIPO_VARIACION_ART` is not null;

-- Recreando vista: vi_articulos_familias
DROP VIEW IF EXISTS `vi_articulos_familias`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_articulos_familias` AS select `fza_articulos_familias`.`CODIGO_FAM_FAM` AS `CODIGO_FAM_FAM`,`fza_articulos_familias`.`ESACTIVO_FAM` AS `ESACTIVO_FAM`,`fza_articulos_familias`.`ORDEN_FAM` AS `ORDEN_FAM`,`fza_articulos_familias`.`ESDEFAULT_FAM` AS `ESDEFAULT_FAM`,`fza_articulos_familias`.`CODIGO_SUBFAMILIA_FAM` AS `CODIGO_SUBFAMILIA_FAM`,`fza_articulos_familias2`.`NOMBRE_FAM_FAM` AS `NOMBRE_SUBFAMILIA`,`fza_articulos_familias`.`NOMBRE_FAM_FAM` AS `NOMBRE_FAM_FAM`,`fza_articulos_familias`.`DESCRIPCION_FAM` AS `DESCRIPCION_FAM`,`fza_articulos_familias`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_articulos_familias`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_articulos_familias`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_articulos_familias`.`USUARIO_MODIF` AS `USUARIO_MODIF` from (`fza_articulos_familias` left join `fza_articulos_familias` `fza_articulos_familias2` on(`fza_articulos_familias`.`CODIGO_SUBFAMILIA_FAM` = `fza_articulos_familias2`.`CODIGO_FAM_FAM`));

-- Recreando vista: vi_articulos_familias_list
DROP VIEW IF EXISTS `vi_articulos_familias_list`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_articulos_familias_list` AS select `fza_articulos_familias`.`CODIGO_FAM_FAM` AS `CODIGO_FAM_FAM`,`fza_articulos_familias`.`ESACTIVO_FAM` AS `ESACTIVO_FAM`,`fza_articulos_familias`.`ORDEN_FAM` AS `ORDEN_FAM`,`fza_articulos_familias`.`ESDEFAULT_FAM` AS `ESDEFAULT_FAM`,`fza_articulos_familias`.`CODIGO_SUBFAMILIA_FAM` AS `CODIGO_SUBFAMILIA_FAM`,`fza_articulos_familias`.`NOMBRE_FAM_FAM` AS `NOMBRE_FAM_FAM`,`fza_articulos_familias`.`DESCRIPCION_FAM` AS `DESCRIPCION_FAM`,`fza_articulos_familias`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_articulos_familias`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_articulos_familias`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_articulos_familias`.`USUARIO_MODIF` AS `USUARIO_MODIF` from `fza_articulos_familias` where `fza_articulos_familias`.`ESACTIVO_FAM` = 'S' order by `fza_articulos_familias`.`ORDEN_FAM`;

-- Recreando vista: vi_articulos_fotos
DROP VIEW IF EXISTS `vi_articulos_fotos`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_articulos_fotos` AS select `sku`.`CODIGO_ART_SKU` AS `CODIGO_ART`,`sku`.`CODIGO_UNIDAD_SKU` AS `CODIGO_UNIDAD_SKU`,coalesce(`fs`.`NOMBRE_FOT_FOT`,`fa`.`NOMBRE_FOT_FOT`) AS `NOMBRE_FOT`,coalesce(`fs`.`EXTENSION_ORIGEN_FOT`,`fa`.`EXTENSION_ORIGEN_FOT`) AS `EXTENSION_ORIGEN_FOT`,case when `fs`.`NOMBRE_FOT_FOT` is not null then 'SKU' when `fa`.`NOMBRE_FOT_FOT` is not null then 'ARTICULO' else NULL end AS `ORIGEN_FOT` from ((`fza_articulos_skus` `sku` left join `fza_articulos_fotos` `fs` on(`fs`.`CODIGO_ART_FOT` = `sku`.`CODIGO_ART_SKU` and `fs`.`CODIGO_UNIDAD_FOT` = `sku`.`CODIGO_UNIDAD_SKU`)) left join `fza_articulos_fotos` `fa` on(`fa`.`CODIGO_ART_FOT` = `sku`.`CODIGO_ART_SKU` and `fa`.`CODIGO_UNIDAD_FOT` = ''));

-- Recreando vista: vi_articulos_list
DROP VIEW IF EXISTS `vi_articulos_list`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_articulos_list` AS select `fza_articulos`.`CODIGO_ART_ART` AS `CODIGO_ART_ART`,`fza_articulos`.`ESACTIVO_ART` AS `ESACTIVO_ART`,`fza_articulos`.`ORDEN_ART` AS `ORDEN_ART`,`fza_articulos`.`DESCRIPCION_ART` AS `DESCRIPCION_ART`,`fza_articulos`.`CODIGO_FAM_ART` AS `CODIGO_FAM_ART`,`fza_articulos_familias`.`DESCRIPCION_FAM` AS `DESCRIPCION_FAM`,`fza_articulos`.`TIPO_IVA_ART` AS `TIPO_IVA_ART`,`fza_articulos`.`ESACTIVO_FIJO_ART` AS `ESACTIVO_FIJO_ART`,`fza_articulos`.`TIPO_CANTIDAD_ART` AS `TIPO_CANTIDAD_ART`,`fza_articulos_proveedores`.`CODIGO_PRV_AP` AS `CODIGO_PRV_PRV`,`fza_proveedores`.`RAZON_SOCIAL_PRV` AS `RAZON_SOCIAL_PRV`,`fza_articulos`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_articulos`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_articulos`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_articulos`.`USUARIO_MODIF` AS `USUARIO_MODIF` from (((`fza_articulos` left join `fza_articulos_familias` on(`fza_articulos`.`CODIGO_FAM_ART` = `fza_articulos_familias`.`CODIGO_FAM_FAM`)) left join `fza_articulos_proveedores` on(`fza_articulos_proveedores`.`CODIGO_ART_AP` = `fza_articulos`.`CODIGO_ART_ART` and `fza_articulos_proveedores`.`ESPROVEEDORPRINCIPAL_AP` = 'S')) left join `fza_proveedores` on(`fza_proveedores`.`CODIGO_PRV_PRV` = `fza_articulos_proveedores`.`CODIGO_PRV_AP`)) where `fza_articulos`.`ESACTIVO_ART` = 'S' order by `fza_articulos`.`ORDEN_ART`;

-- Recreando vista: vi_articulos_pdte_recibir
DROP VIEW IF EXISTS `vi_articulos_pdte_recibir`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_articulos_pdte_recibir` AS select `fza_articulos_pdte_recibir`.`CODIGO_UNIDAD_PDR` AS `CODIGO_UNIDAD_SKU`,`fza_articulos_pdte_recibir`.`CODIGO_ALM_PDR` AS `CODIGO_ALM_ALM`,`fza_articulos_pdte_recibir`.`CODIGO_ART_PDR` AS `CODIGO_ART_ART`,sum(`fza_articulos_pdte_recibir`.`CANTIDAD_PDR`) AS `CANTIDAD_PTE_RECIBIR`,count(0) AS `NUM_LINEAS_PDR`,min(`fza_articulos_pdte_recibir`.`FECHA_PEDIDO_PDR`) AS `FECHA_PEDIDO_MIN`,min(`fza_articulos_pdte_recibir`.`FECHA_PREVISTA_PDR`) AS `FECHA_PREVISTA_MIN` from `fza_articulos_pdte_recibir` group by `fza_articulos_pdte_recibir`.`CODIGO_UNIDAD_PDR`,`fza_articulos_pdte_recibir`.`CODIGO_ALM_PDR`,`fza_articulos_pdte_recibir`.`CODIGO_ART_PDR`;

-- Recreando vista: vi_articulos_propiedades_slots
DROP VIEW IF EXISTS `vi_articulos_propiedades_slots`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_articulos_propiedades_slots` AS select `a`.`CODIGO_ART_ART` AS `CODIGO_ART_ART`,`fa`.`CODIGO_FAM_FAM` AS `CODIGO_FAM_FAM`,`p`.`CODIGO_PROP_ARTPROP` AS `CODIGO_PROP_ARTPROP`,`p`.`NOMBRE_PROP_PROP` AS `NOMBRE_PROP_PROP`,`p`.`TIPO_VALOR_PROP` AS `TIPO_VALOR_PROP`,`fa`.`ESREQUERIDO_FA` AS `ESREQUERIDO_FA`,`fa`.`ORDEN_MOSTRAR_FA` AS `ORDEN_MOSTRAR_FA`,`ap`.`ID_PV_ARTPROP` AS `ID_PV_ARTPROP`,`ap`.`VALOR_LIBRE_ARTPROP` AS `VALOR_LIBRE_ARTPROP`,coalesce(`pv`.`PV`,`ap`.`VALOR_LIBRE_ARTPROP`) AS `VALOR_FINAL_MOSTRAR` from ((((`fza_articulos` `a` join `fza_familias_atributos` `fa` on(`a`.`CODIGO_FAM_ART` = `fa`.`CODIGO_FAM_FAM`)) join `fza_propiedades` `p` on(`fa`.`CODIGO_PROP_ARTPROP` = `p`.`CODIGO_PROP_ARTPROP`)) left join `fza_articulos_propiedades` `ap` on(`a`.`CODIGO_ART_ART` = `ap`.`CODIGO_ART_ART` and `p`.`CODIGO_PROP_ARTPROP` = `ap`.`CODIGO_PROP_ARTPROP`)) left join `fza_propiedades_valores` `pv` on(`ap`.`ID_PV_ARTPROP` = `pv`.`ID_PV_ARTPROP`));

-- Recreando vista: vi_articulos_proveedores
DROP VIEW IF EXISTS `vi_articulos_proveedores`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_articulos_proveedores` AS select `ap`.`CODIGO_PRV_AP` AS `CODIGO_PRV_PRV`,`ap`.`CODIGO_ART_AP` AS `CODIGO_ART_ART`,`p`.`RAZON_SOCIAL_PRV` AS `RAZON_SOCIAL_PRV`,`ap`.`REF_PROVEEDOR_AP` AS `REF_PROVEEDOR`,`ap`.`PRECIO_ULT_COMPRA_AP` AS `PRECIO_ULT_COMPRA`,`ap`.`FECHA_VALIDEZ_AP` AS `FECHA_VALIDEZ`,`ap`.`ESPROVEEDORPRINCIPAL_AP` AS `ESPROVEEDORPRINCIPAL`,`ap`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`ap`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`ap`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`ap`.`USUARIO_MODIF` AS `USUARIO_MODIF` from (`fza_articulos_proveedores` `ap` left join `fza_proveedores` `p` on(`ap`.`CODIGO_PRV_AP` = `p`.`CODIGO_PRV_PRV`));

-- Recreando vista: vi_articulos_skus
DROP VIEW IF EXISTS `vi_articulos_skus`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_articulos_skus` AS select `fza_articulos_skus`.`CODIGO_UNIDAD_SKU` AS `CODIGO_UNIDAD_SKU`,`fza_articulos_skus`.`CODIGO_ART_SKU` AS `CODIGO_ART_SKU`,`fza_articulos_skus`.`CODIGO_VAR_SKU` AS `CODIGO_VAR_SKU`,`fza_articulos_skus`.`ESACTIVO_SKU` AS `ESACTIVO_SKU`,`fza_articulos_skus`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_articulos_skus`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_articulos_skus`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_articulos_skus`.`USUARIO_MODIF` AS `USUARIO_MODIF` from `fza_articulos_skus`;

-- Recreando vista: vi_articulos_skus_con_coste
DROP VIEW IF EXISTS `vi_articulos_skus_con_coste`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_articulos_skus_con_coste` AS select `sku`.`CODIGO_UNIDAD_SKU` AS `CODIGO_UNIDAD_SKU`,`sku`.`CODIGO_ART_SKU` AS `CODIGO_ART_SKU`,`sku`.`CODIGO_VAR_SKU` AS `CODIGO_VAR_SKU`,`sku`.`ESACTIVO_SKU` AS `ESACTIVO_SKU`,`skuc`.`PRECIO_ULT_COMPRA_SKUC` AS `PRECIO_ULT_COMPRA_SKUC`,`skuc`.`FECHA_ULT_COMPRA_SKUC` AS `FECHA_ULT_COMPRA_SKUC`,`sku`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`sku`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`sku`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`sku`.`USUARIO_MODIF` AS `USUARIO_MODIF` from (`fza_articulos_skus` `sku` left join `fza_articulos_skus_costes` `skuc` on(`skuc`.`CODIGO_UNIDAD_SKU_SKUC` = `sku`.`CODIGO_UNIDAD_SKU`));

-- Recreando vista: vi_articulos_skus_etiquetas
DROP VIEW IF EXISTS `vi_articulos_skus_etiquetas`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_articulos_skus_etiquetas` AS with sku_atrib as (select `sa`.`CODIGO_UNIDAD_SKU_SA` AS `CODIGO_UNIDAD_SKU`,max(case when `av`.`ID_VA_AV` = 'CO' then `av`.`AV` end) AS `ATR_CO`,max(case when `av`.`ID_VA_AV` = 'TAL' then `av`.`AV` end) AS `ATR_TAL`,group_concat(concat(coalesce(`va`.`NOMBRE_VA`,`av`.`ID_VA_AV`),': ',`av`.`AV`) order by coalesce(`va`.`ORDEN_VA`,99) ASC,`av`.`ID_VA_AV` ASC separator ' / ') AS `ATRIBUTOS_TXT`,group_concat(`av`.`AV` order by coalesce(`va`.`ORDEN_VA`,99) ASC,`av`.`ID_VA_AV` ASC separator ' / ') AS `DESCRIPCION_SKU` from ((`fza_atributos_sku` `sa` join `fza_atributos_valores` `av` on(`av`.`ID_AV` = `sa`.`ID_AV_SA`)) left join `fza_variaciones_atributos` `va` on(`va`.`ID_ATB_VA` = `av`.`ID_VA_AV`)) group by `sa`.`CODIGO_UNIDAD_SKU_SA`), art_prop as (select `ap`.`CODIGO_ART_ART` AS `CODIGO_ART_ART`,max(case when `ap`.`CODIGO_PROP_ARTPROP` = 'MARCA' then coalesce(`pv`.`PV`,`ap`.`VALOR_LIBRE_ARTPROP`) end) AS `PROP_MARCA`,max(case when `ap`.`CODIGO_PROP_ARTPROP` = 'MATERIAL' then coalesce(`pv`.`PV`,`ap`.`VALOR_LIBRE_ARTPROP`) end) AS `PROP_MATERIAL`,max(case when `ap`.`CODIGO_PROP_ARTPROP` = 'TEMPORADA' then coalesce(`pv`.`PV`,`ap`.`VALOR_LIBRE_ARTPROP`) end) AS `PROP_TEMPORADA`,max(case when `ap`.`CODIGO_PROP_ARTPROP` = 'GENERO' then coalesce(`pv`.`PV`,`ap`.`VALOR_LIBRE_ARTPROP`) end) AS `PROP_GENERO`,max(case when `ap`.`CODIGO_PROP_ARTPROP` = 'ESTILO' then coalesce(`pv`.`PV`,`ap`.`VALOR_LIBRE_ARTPROP`) end) AS `PROP_ESTILO`,max(case when `ap`.`CODIGO_PROP_ARTPROP` = 'ORIGEN' then coalesce(`pv`.`PV`,`ap`.`VALOR_LIBRE_ARTPROP`) end) AS `PROP_ORIGEN`,max(case when `ap`.`CODIGO_PROP_ARTPROP` = 'COMPOSICION' then coalesce(`pv`.`PV`,`ap`.`VALOR_LIBRE_ARTPROP`) end) AS `PROP_COMPOSICION`,group_concat(concat(coalesce(`p`.`NOMBRE_PROP_PROP`,`ap`.`CODIGO_PROP_ARTPROP`),': ',coalesce(`pv`.`PV`,`ap`.`VALOR_LIBRE_ARTPROP`,'')) order by `ap`.`CODIGO_PROP_ARTPROP` ASC separator ' | ') AS `PROPIEDADES_TXT` from ((`fza_articulos_propiedades` `ap` left join `fza_propiedades` `p` on(`p`.`CODIGO_PROP_ARTPROP` = `ap`.`CODIGO_PROP_ARTPROP`)) left join `fza_propiedades_valores` `pv` on(`pv`.`ID_PV_ARTPROP` = `ap`.`ID_PV_ARTPROP`)) group by `ap`.`CODIGO_ART_ART`), cb_prin as (select `fza_codigos_barras`.`CODIGO_UNIDAD_CB` AS `CODIGO_UNIDAD_CB`,max(`fza_codigos_barras`.`CODIGO_BARRAS_CB`) AS `CODIGO_BARRAS_CB` from `fza_codigos_barras` where `fza_codigos_barras`.`ESPRINCIPAL_CB` = 'S' group by `fza_codigos_barras`.`CODIGO_UNIDAD_CB`)select `sku`.`CODIGO_UNIDAD_SKU` AS `CODIGO_UNIDAD_SKU`,`sku`.`CODIGO_ART_SKU` AS `CODIGO_ART_ART`,`sku`.`CODIGO_VAR_SKU` AS `CODIGO_VAR_SKU`,`sku`.`ESACTIVO_SKU` AS `ESACTIVO_SKU`,`art`.`ESACTIVO_ART` AS `ESACTIVO_ART`,`art`.`DESCRIPCION_ART` AS `DESCRIPCION_ART`,`art`.`TIPO_ART` AS `TIPO_ART`,`art`.`TIPO_IVA_ART` AS `TIPO_IVA_ART`,`art`.`CODIGO_FAM_ART` AS `CODIGO_FAM_ART`,`fam`.`NOMBRE_FAM_FAM` AS `NOMBRE_FAM_FAM`,`fam`.`DESCRIPCION_FAM` AS `DESCRIPCION_FAM`,`sa`.`ATR_CO` AS `ATR_CO`,`sa`.`ATR_TAL` AS `ATR_TAL`,`sa`.`ATRIBUTOS_TXT` AS `ATRIBUTOS_TXT`,`sa`.`DESCRIPCION_SKU` AS `DESCRIPCION_SKU`,`apr`.`PROP_MARCA` AS `PROP_MARCA`,`apr`.`PROP_MATERIAL` AS `PROP_MATERIAL`,`apr`.`PROP_TEMPORADA` AS `PROP_TEMPORADA`,`apr`.`PROP_GENERO` AS `PROP_GENERO`,`apr`.`PROP_ESTILO` AS `PROP_ESTILO`,`apr`.`PROP_ORIGEN` AS `PROP_ORIGEN`,`apr`.`PROP_COMPOSICION` AS `PROP_COMPOSICION`,`apr`.`PROPIEDADES_TXT` AS `PROPIEDADES_TXT`,`ap`.`CODIGO_PRV_AP` AS `CODIGO_PRV_PRV`,`prv`.`RAZON_SOCIAL_PRV` AS `RAZON_SOCIAL_PRV`,`ap`.`REF_PROVEEDOR_AP` AS `REF_PROVEEDOR`,`cb`.`CODIGO_BARRAS_CB` AS `CODIGO_BARRAS_CB` from (((((((`fza_articulos_skus` `sku` join `fza_articulos` `art` on(`art`.`CODIGO_ART_ART` = `sku`.`CODIGO_ART_SKU`)) left join `fza_articulos_familias` `fam` on(`fam`.`CODIGO_FAM_FAM` = `art`.`CODIGO_FAM_ART`)) left join `sku_atrib` `sa` on(`sa`.`CODIGO_UNIDAD_SKU` = `sku`.`CODIGO_UNIDAD_SKU`)) left join `art_prop` `apr` on(`apr`.`CODIGO_ART_ART` = `sku`.`CODIGO_ART_SKU`)) left join `fza_articulos_proveedores` `ap` on(`ap`.`CODIGO_ART_AP` = `sku`.`CODIGO_ART_SKU` and `ap`.`ESPROVEEDORPRINCIPAL_AP` = 'S')) left join `fza_proveedores` `prv` on(`prv`.`CODIGO_PRV_PRV` = `ap`.`CODIGO_PRV_AP`)) left join `cb_prin` `cb` on(`cb`.`CODIGO_UNIDAD_CB` = `sku`.`CODIGO_UNIDAD_SKU`));

-- Recreando vista: vi_articulos_skus_extendida
DROP VIEW IF EXISTS `vi_articulos_skus_extendida`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_articulos_skus_extendida` AS select `sku`.`CODIGO_UNIDAD_SKU` AS `CODIGO_UNIDAD_SKU`,`sku`.`CODIGO_ART_SKU` AS `CODIGO_ART_SKU`,`sku`.`CODIGO_VAR_SKU` AS `CODIGO_VAR_SKU`,`sku`.`ESACTIVO_SKU` AS `ESACTIVO_SKU`,`cb`.`ID_CB` AS `ID_CB`,`cb`.`CODIGO_BARRAS_CB` AS `CODIGO_BARRAS_CB`,`cb`.`TIPO_CODIGO_CB` AS `TIPO_CODIGO_CB`,`cb`.`ESPRINCIPAL_CB` AS `ESPRINCIPAL_CB`,coalesce(`stk`.`STOCK_TOTAL`,0) AS `STOCK_TOTAL`,`sku`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`sku`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`sku`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`sku`.`USUARIO_MODIF` AS `USUARIO_MODIF` from ((`fza_articulos_skus` `sku` join `fza_codigos_barras` `cb` on(`cb`.`CODIGO_UNIDAD_CB` = `sku`.`CODIGO_UNIDAD_SKU`)) left join (select `fza_articulos_stockactual`.`CODIGO_UNIDAD_STK` AS `CODIGO_UNIDAD_STK`,sum(`fza_articulos_stockactual`.`CANTIDAD_STK`) AS `STOCK_TOTAL` from `fza_articulos_stockactual` group by `fza_articulos_stockactual`.`CODIGO_UNIDAD_STK`) `stk` on(`sku`.`CODIGO_UNIDAD_SKU` = `stk`.`CODIGO_UNIDAD_STK`));

-- Recreando vista: vi_articulos_tarifas
DROP VIEW IF EXISTS `vi_articulos_tarifas`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_articulos_tarifas` AS select `at`.`CODIGO_ART_ARTTAR` AS `CODIGO_ART_ARTTAR`,coalesce(`at`.`CODIGO_UNIDAD_ARTTAR`,'') AS `CODIGO_UNIDAD_ARTTAR`,`at`.`CODIGO_TAR_ARTTAR` AS `CODIGO_TAR_ARTTAR`,`t`.`NOMBRE_TAR_TAR` AS `NOMBRE_TAR_TAR`,`at`.`CODIGO_UNICO_ARTTAR` AS `CODIGO_UNICO_ARTTAR`,case when coalesce(`at`.`CODIGO_UNIDAD_ARTTAR`,'') <> '' then `at`.`CODIGO_UNICO_ARTTAR` end AS `CODIGO_UNICO_TARIFA_SKU`,case when coalesce(`at`.`CODIGO_UNIDAD_ARTTAR`,'') = '' then `at`.`CODIGO_UNICO_ARTTAR` end AS `CODIGO_UNICO_TARIFA_PADRE`,case when coalesce(`at`.`CODIGO_UNIDAD_ARTTAR`,'') <> '' then 'ESPECIFICO_SKU' else 'PADRE' end AS `ORIGEN_PRECIO`,`at`.`PRECIO_SALIDA_ARTTAR` AS `PRECIO_SALIDA_ARTTAR`,`at`.`PRECIO_FINAL_ARTTAR` AS `PRECIO_FINAL_ARTTAR`,`at`.`PRECIO_DTO_ARTTAR` AS `PRECIO_DTO_ARTTAR`,`at`.`PORCENTAJE_DTO_ARTTAR` AS `PORCENTAJE_DTO_ARTTAR`,`at`.`PORCENTAJE_MARGEN_ARTTAR` AS `PORCENTAJE_MARGEN_ARTTAR`,`at`.`VALOR_MULTIPLO_AJUSTE_ARTTAR` AS `VALOR_MULTIPLO_AJUSTE_ARTTAR`,`at`.`VALOR_MENOS_AJUSTE_ARTTAR` AS `VALOR_MENOS_AJUSTE_ARTTAR`,coalesce(`at`.`PORCENTAJE_MARGEN_ARTTAR`,`t`.`PORCENTAJE_MARGEN_TAR`) AS `PORCENTAJE_MARGEN_EFECTIVO`,coalesce(`at`.`VALOR_MULTIPLO_AJUSTE_ARTTAR`,`t`.`VALOR_MULTIPLO_AJUSTE_TAR`) AS `VALOR_MULTIPLO_AJUSTE_EFECTIVO`,coalesce(`at`.`VALOR_MENOS_AJUSTE_ARTTAR`,`t`.`VALOR_MENOS_AJUSTE_TAR`) AS `VALOR_MENOS_AJUSTE_EFECTIVO`,`at`.`FECHA_DESDE_ARTTAR` AS `FECHA_DESDE_ARTTAR`,`at`.`FECHA_HASTA_ARTTAR` AS `FECHA_HASTA_ARTTAR`,`t`.`ESACTIVO_ARTTAR` AS `ESACTIVO_ARTTAR`,`t`.`ESIMP_INCL_TAR` AS `ESIMP_INCL_TAR`,`a`.`DESCRIPCION_ART` AS `DESCRIPCION_ART`,`a`.`TIPO_CANTIDAD_ART` AS `TIPO_CANTIDAD_ART`,`a`.`ESVARIACION_ART` AS `ESVARIACION_ART`,`iv`.`CODIGO_ABREVIATURA_IVA_IVATIP` AS `TIPO_IVA_ARTICULO`,case when `tiene_sku`.`CODIGO_ART_SKU` is not null then 'S' else 'N' end AS `TIENE_SKU`,`sku`.`ESACTIVO_SKU` AS `ESACTIVO_SKU`,(select group_concat(`av`.`AV` order by `av`.`ORDEN_AV` ASC separator ' / ') from (`fza_atributos_sku` `sa` join `fza_atributos_valores` `av` on(`av`.`ID_AV` = `sa`.`ID_AV_SA`)) where `sa`.`CODIGO_UNIDAD_SKU_SA` = `at`.`CODIGO_UNIDAD_ARTTAR`) AS `DESCRIPCION_SKU`,`ap`.`CODIGO_PRV_AP` AS `CODIGO_PRV_PRV`,`p`.`RAZON_SOCIAL_PRV` AS `RAZON_SOCIAL_PRV`,case when coalesce(`at`.`CODIGO_UNIDAD_ARTTAR`,'') <> '' then `skuc`.`PRECIO_ULT_COMPRA_SKUC` else `ap`.`PRECIO_ULT_COMPRA_AP` end AS `PRECIO_ULT_COMPRA`,case when coalesce(`at`.`CODIGO_UNIDAD_ARTTAR`,'') <> '' then `skuc`.`FECHA_ULT_COMPRA_SKUC` else `ap`.`FECHA_VALIDEZ_AP` end AS `FECHA_VALIDEZ`,`a`.`CODIGO_FAM_ART` AS `CODIGO_FAM_ART`,`af`.`DESCRIPCION_FAM` AS `DESCRIPCION_FAM`,coalesce(`num_atr`.`NUM_ATRIBUTOS_REQ`,0) AS `NUM_ATRIBUTOS_REQ`,`at`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`at`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`at`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`at`.`USUARIO_MODIF` AS `USUARIO_MODIF` from ((((((((((`fza_articulos_tarifas` `at` join `fza_articulos` `a` on(`a`.`CODIGO_ART_ART` = `at`.`CODIGO_ART_ARTTAR`)) join `fza_tarifas` `t` on(`t`.`CODIGO_TAR_ARTTAR` = `at`.`CODIGO_TAR_ARTTAR`)) left join `fza_articulos_skus` `sku` on(`sku`.`CODIGO_UNIDAD_SKU` = `at`.`CODIGO_UNIDAD_ARTTAR` and coalesce(`at`.`CODIGO_UNIDAD_ARTTAR`,'') <> '')) left join `fza_articulos_skus_costes` `skuc` on(`skuc`.`CODIGO_UNIDAD_SKU_SKUC` = `at`.`CODIGO_UNIDAD_ARTTAR` and coalesce(`at`.`CODIGO_UNIDAD_ARTTAR`,'') <> '')) left join `fza_articulos_proveedores` `ap` on(`ap`.`CODIGO_ART_AP` = `a`.`CODIGO_ART_ART` and `ap`.`ESPROVEEDORPRINCIPAL_AP` = 'S')) left join `fza_proveedores` `p` on(`p`.`CODIGO_PRV_PRV` = `ap`.`CODIGO_PRV_AP`)) left join `fza_articulos_familias` `af` on(`af`.`CODIGO_FAM_FAM` = `a`.`CODIGO_FAM_ART`)) left join `fza_ivas_tipos` `iv` on(`iv`.`CODIGO_ABREVIATURA_IVA_IVATIP` = `a`.`TIPO_IVA_ART`)) left join (select distinct `fza_articulos_skus`.`CODIGO_ART_SKU` AS `CODIGO_ART_SKU` from `fza_articulos_skus` where `fza_articulos_skus`.`ESACTIVO_SKU` = 'S') `tiene_sku` on(`tiene_sku`.`CODIGO_ART_SKU` = `a`.`CODIGO_ART_ART`)) left join (select `sk`.`CODIGO_ART_SKU` AS `CODIGO_ART_SKU`,count(distinct `va`.`ID_ATB_VA`) AS `NUM_ATRIBUTOS_REQ` from (`fza_articulos_skus` `sk` join `fza_variaciones_atributos` `va` on(`va`.`ID_VAR_VA` = `sk`.`CODIGO_VAR_SKU`)) group by `sk`.`CODIGO_ART_SKU`) `num_atr` on(`num_atr`.`CODIGO_ART_SKU` = `a`.`CODIGO_ART_ART`)) where `at`.`ESACTIVO_ARTTAR` = 'S' and `t`.`ESACTIVO_ARTTAR` = 'S' and (`at`.`FECHA_HASTA_ARTTAR` is null or `at`.`FECHA_HASTA_ARTTAR` >= curdate()) order by `t`.`ORDEN_TAR`,`a`.`ORDEN_ART`,`at`.`CODIGO_UNIDAD_ARTTAR`;

-- Recreando vista: vi_art_busquedas
DROP VIEW IF EXISTS `vi_art_busquedas`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_art_busquedas` AS select `a`.`CODIGO_ART_ART` AS `CODIGO_ART_ART`,`a`.`ESACTIVO_ART` AS `ESACTIVO_ART`,`a`.`DESCRIPCION_ART` AS `DESCRIPCION_ART`,`a`.`CODIGO_FAM_ART` AS `CODIGO_FAM_ART`,`af`.`DESCRIPCION_FAM` AS `DESCRIPCION_FAM`,`ap`.`CODIGO_PRV_AP` AS `CODIGO_PRV_PRV`,`p`.`RAZON_SOCIAL_PRV` AS `RAZON_SOCIAL_PROVEEDOR`,`ap`.`ESPROVEEDORPRINCIPAL_AP` AS `ESPROVEEDORPRINCIPAL`,`ap`.`PRECIO_ULT_COMPRA_AP` AS `PRECIO_ULT_COMPRA`,`at2`.`CODIGO_TAR_ARTTAR` AS `CODIGO_TAR_ARTTAR`,`t`.`NOMBRE_TAR_TAR` AS `NOMBRE_TAR_TAR`,`at2`.`PRECIO_SALIDA_ARTTAR` AS `PRECIO_SALIDA_ARTTAR`,`at2`.`PRECIO_DTO_ARTTAR` AS `PRECIO_DTO_ARTTAR`,`at2`.`PORCENTAJE_DTO_ARTTAR` AS `PORCENTAJE_DTO_ARTTAR`,`at2`.`PRECIO_FINAL_ARTTAR` AS `PRECIO_FINAL_ARTTAR`,`at2`.`FECHA_DESDE_ARTTAR` AS `FECHA_DESDE_ARTTAR`,`at2`.`FECHA_HASTA_ARTTAR` AS `FECHA_HASTA_ARTTAR`,`t`.`ESIMP_INCL_TAR` AS `ESIMP_INCL_TAR`,`iv`.`NOMBRE_TIPO_IVA_IVATIP` AS `NOMBRE_TIPO_IVA_IVATIP`,`a`.`TIPO_IVA_ART` AS `TIPO_IVA_ART`,`a`.`TIPO_CANTIDAD_ART` AS `TIPO_CANTIDAD_ART`,`a`.`USUARIO_MODIF` AS `USUARIO_MODIF`,`a`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`a`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`a`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`a`.`ESACTIVO_FIJO_ART` AS `ESACTIVO_FIJO_ART` from ((((((`fza_articulos` `a` left join `fza_articulos_familias` `af` on(`af`.`CODIGO_FAM_FAM` = `a`.`CODIGO_FAM_ART`)) left join `fza_articulos_tarifas` `at2` on(`at2`.`CODIGO_ART_ARTTAR` = `a`.`CODIGO_ART_ART` and ifnull(`at2`.`CODIGO_UNIDAD_ARTTAR`,'') = '' and `at2`.`ESACTIVO_ARTTAR` = 'S')) left join `fza_tarifas` `t` on(`t`.`CODIGO_TAR_ARTTAR` = `at2`.`CODIGO_TAR_ARTTAR` and `t`.`ESACTIVO_ARTTAR` = 'S' and `t`.`ORDEN_TAR` = (select min(`t2`.`ORDEN_TAR`) from `fza_tarifas` `t2` where `t2`.`ESACTIVO_ARTTAR` = 'S'))) left join `fza_ivas_tipos` `iv` on(`iv`.`CODIGO_ABREVIATURA_IVA_IVATIP` = `a`.`TIPO_IVA_ART`)) left join `fza_articulos_proveedores` `ap` on(`ap`.`CODIGO_ART_AP` = `a`.`CODIGO_ART_ART` and `ap`.`ESPROVEEDORPRINCIPAL_AP` = 'S')) left join `fza_proveedores` `p` on(`p`.`CODIGO_PRV_PRV` = `ap`.`CODIGO_PRV_AP`)) where `a`.`ESACTIVO_ART` = 'S' order by `a`.`ORDEN_ART`;

-- Recreando vista: vi_atributos_nombres
DROP VIEW IF EXISTS `vi_atributos_nombres`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_atributos_nombres` AS select `ask`.`CODIGO_ART_SKU` AS `CODIGO_ART_PADRE_ARTVIN`,`vat`.`ID_ATB_VA` AS `ID_ATRIBUTO`,`vat`.`NOMBRE_VA` AS `NOMBRE_ATRIBUTO`,`vat`.`ORDEN_VA` AS `ORDEN_VISUAL_ATRIBUTO` from (`fza_articulos_skus` `ask` join `fza_variaciones_atributos` `vat` on(`vat`.`ID_VAR_VA` = `ask`.`CODIGO_VAR_SKU`));

-- Recreando vista: vi_atributos_sku_basico
DROP VIEW IF EXISTS `vi_atributos_sku_basico`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_atributos_sku_basico` AS select `sku`.`CODIGO_ART_SKU` AS `CODIGO_ART_SKU`,`sku`.`CODIGO_UNIDAD_SKU` AS `CODIGO_UNIDAD_SKU`,`sku`.`CODIGO_VAR_SKU` AS `CODIGO_VAR_SKU`,`val`.`ID_AV` AS `ID_AV`,`val`.`ID_VA_AV` AS `ID_VA_AV`,`va`.`NOMBRE_VA` AS `NOMBRE_ATRIBUTO`,`va`.`ORDEN_VA` AS `ORDEN_ATRIBUTO`,`val`.`AV` AS `VALOR_AV`,`val`.`DESCRIPCION_AV` AS `DESCRIPCION_AV`,`aca`.`ID_AC_ACA` AS `ID_AC`,`aab`.`ID_ATB_AAB` AS `ID_ATB_OVERRIDE`,`acd`.`ID_ATB_ACD` AS `ID_ATB_CONJUNTO`,`val`.`ID_ATB_AV` AS `ID_ATB_GLOBAL`,case when `aab`.`CODIGO_ART_AAB` is not null then `aab`.`ID_ATB_AAB` when `acd`.`ID_ATB_ACD` is not null then `acd`.`ID_ATB_ACD` else `val`.`ID_ATB_AV` end AS `ID_ATB_AV`,case when `aab`.`CODIGO_ART_AAB` is not null then 'A' when `acd`.`ID_ATB_ACD` is not null then 'C' when `val`.`ID_ATB_AV` is not null then 'G' else NULL end AS `FUENTE_ATB`,`atb`.`CODIGO_ATB` AS `CODIGO_ATB`,`atb`.`NOMBRE_ATB` AS `NOMBRE_ATB`,`atb`.`DESCRIPCION_ATB` AS `DESCRIPCION_ATB`,`atb`.`HEX_ATB` AS `HEX_ATB`,`atb`.`VALOR_NUM_ATB` AS `VALOR_NUM_ATB`,`atb`.`UNIDAD_ATB` AS `UNIDAD_ATB`,case when `atb`.`VALOR_NUM_ATB` is not null then concat(trim(trailing '0' from trim(trailing '.' from cast(`atb`.`VALOR_NUM_ATB` as char charset utf8mb4))),coalesce(concat(' ',`atb`.`UNIDAD_ATB`),'')) when `atb`.`HEX_ATB` is not null then concat(`atb`.`NOMBRE_ATB`,' ',`atb`.`HEX_ATB`) else `atb`.`NOMBRE_ATB` end AS `ETIQUETA_BASICO` from (((((((`fza_articulos_skus` `sku` join `fza_atributos_sku` `sa` on(`sa`.`CODIGO_UNIDAD_SKU_SA` = `sku`.`CODIGO_UNIDAD_SKU`)) join `fza_atributos_valores` `val` on(`val`.`ID_AV` = `sa`.`ID_AV_SA`)) left join `fza_variaciones_atributos` `va` on(`va`.`ID_VAR_VA` = `sku`.`CODIGO_VAR_SKU` and `va`.`ID_ATB_VA` = `val`.`ID_VA_AV`)) left join `fza_articulos_atributos_basicos` `aab` on(`aab`.`CODIGO_ART_AAB` = `sku`.`CODIGO_ART_SKU` and `aab`.`ID_AV_AAB` = `val`.`ID_AV`)) left join `fza_articulos_conjuntos_asign` `aca` on(`aca`.`CODIGO_ART_ACA` = `sku`.`CODIGO_ART_SKU` and `aca`.`ID_VA_ACA` = `val`.`ID_VA_AV`)) left join `fza_atributos_conjuntos_det` `acd` on(`acd`.`ID_AC_ACD` = `aca`.`ID_AC_ACA` and `acd`.`ID_AV_ACD` = `val`.`ID_AV`)) left join `fza_atributos_basicos` `atb` on(`atb`.`ID_ATB` = case when `aab`.`CODIGO_ART_AAB` is not null then `aab`.`ID_ATB_AAB` when `acd`.`ID_ATB_ACD` is not null then `acd`.`ID_ATB_ACD` else `val`.`ID_ATB_AV` end)) union all select `sku`.`CODIGO_ART_SKU` AS `CODIGO_ART_SKU`,`sku`.`CODIGO_UNIDAD_SKU` AS `CODIGO_UNIDAD_SKU`,`sku`.`CODIGO_VAR_SKU` AS `CODIGO_VAR_SKU`,NULL AS `ID_AV`,`va`.`ID_ATB_VA` AS `ID_VA_AV`,`va`.`NOMBRE_VA` AS `NOMBRE_ATRIBUTO`,`va`.`ORDEN_VA` AS `ORDEN_ATRIBUTO`,substring_index(substring_index(substr(`sku`.`CODIGO_UNIDAD_SKU`,char_length(`sku`.`CODIGO_ART_SKU`) + 2),'/',`va`.`ORDEN_VA`),'/',-1) AS `VALOR_AV`,NULL AS `DESCRIPCION_AV`,NULL AS `ID_AC`,NULL AS `ID_ATB_OVERRIDE`,NULL AS `ID_ATB_CONJUNTO`,NULL AS `ID_ATB_GLOBAL`,NULL AS `ID_ATB_AV`,NULL AS `FUENTE_ATB`,NULL AS `CODIGO_ATB`,NULL AS `NOMBRE_ATB`,NULL AS `DESCRIPCION_ATB`,NULL AS `HEX_ATB`,NULL AS `VALOR_NUM_ATB`,NULL AS `UNIDAD_ATB`,NULL AS `ETIQUETA_BASICO` from (`fza_articulos_skus` `sku` join `fza_variaciones_atributos` `va` on(`va`.`ID_VAR_VA` = `sku`.`CODIGO_VAR_SKU`)) where `sku`.`CODIGO_UNIDAD_SKU` like concat(`sku`.`CODIGO_ART_SKU`,'/%') and !exists(select 1 from (`fza_atributos_sku` `sa` join `fza_atributos_valores` `v` on(`v`.`ID_AV` = `sa`.`ID_AV_SA`)) where `sa`.`CODIGO_UNIDAD_SKU_SA` = `sku`.`CODIGO_UNIDAD_SKU` and `v`.`ID_VA_AV` = `va`.`ID_ATB_VA` limit 1);

-- Recreando vista: vi_cajasdef
DROP VIEW IF EXISTS `vi_cajasdef`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_cajasdef` AS select `e`.`CODIGO_EMP_EMP` AS `Empresa`,`e`.`RAZON_SOCIAL_EMP` AS `NombreEmpresa`,`a`.`CODIGO_ALM_ALM` AS `Almacen`,`a`.`NOMBRE_ALM_ALM` AS `NombreAlmacén`,`c`.`CODIGO_CAJA_ALMCAJ` AS `Caja`,`c`.`DESCRIPCION_ALMCAJ` AS `NombreCaja` from ((`fza_empresas` `e` join `fza_almacenes` `a` on(`e`.`CODIGO_EMP_EMP` = `a`.`CODIGO_EMP_ALM`)) join `fza_almacenes_cajas` `c` on(`a`.`CODIGO_ALM_ALM` = `c`.`CODIGO_ALM_ALMCAJ`)) where `e`.`ESACTIVO_EMP` = 'S' and `a`.`ESACTIVO_ALM` = 'S';

-- Recreando vista: vi_caja_busqueda_unificada
DROP VIEW IF EXISTS `vi_caja_busqueda_unificada`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_caja_busqueda_unificada` AS select `a`.`CODIGO_ART_ART` AS `INPUT_BUSQUEDA`,'CODIGO' AS `TIPO_COINCIDENCIA`,`a`.`CODIGO_ART_ART` AS `CODIGO_PADRE`,NULL AS `CODIGO_SKU`,`a`.`DESCRIPCION_ART` AS `DESCRIPCION_ART`,`a`.`TIPO_ART` AS `TIPO_ART` from `fza_articulos` `a` where `a`.`ESACTIVO_ART` = 'S' union all select `sku`.`CODIGO_UNIDAD_SKU` AS `CODIGO_UNIDAD_SKU`,'SKU' AS `SKU`,`a`.`CODIGO_ART_ART` AS `CODIGO_ART_ART`,`sku`.`CODIGO_UNIDAD_SKU` AS `CODIGO_UNIDAD_SKU`,`a`.`DESCRIPCION_ART` AS `DESCRIPCION_ART`,`a`.`TIPO_ART` AS `TIPO_ART` from (`fza_articulos_skus` `sku` join `fza_articulos` `a` on(`sku`.`CODIGO_ART_SKU` = `a`.`CODIGO_ART_ART`)) where `sku`.`ESACTIVO_SKU` = 'S' union all select `cb`.`CODIGO_BARRAS_CB` AS `CODIGO_BARRAS_CB`,'EAN' AS `EAN`,`a`.`CODIGO_ART_ART` AS `CODIGO_ART_ART`,`sku`.`CODIGO_UNIDAD_SKU` AS `CODIGO_UNIDAD_SKU`,`a`.`DESCRIPCION_ART` AS `DESCRIPCION_ART`,`a`.`TIPO_ART` AS `TIPO_ART` from ((`fza_codigos_barras` `cb` join `fza_articulos_skus` `sku` on(`cb`.`CODIGO_UNIDAD_CB` = `sku`.`CODIGO_UNIDAD_SKU`)) join `fza_articulos` `a` on(`sku`.`CODIGO_ART_SKU` = `a`.`CODIGO_ART_ART`)) union all select `ap`.`REF_PROVEEDOR_AP` AS `REF_PROVEEDOR_AP`,'MODELO_PROV' AS `MODELO_PROV`,`a`.`CODIGO_ART_ART` AS `CODIGO_ART_ART`,NULL AS `NULL`,`a`.`DESCRIPCION_ART` AS `DESCRIPCION_ART`,`a`.`TIPO_ART` AS `TIPO_ART` from (`fza_articulos_proveedores` `ap` join `fza_articulos` `a` on(`ap`.`CODIGO_ART_AP` = `a`.`CODIGO_ART_ART`)) where `a`.`ESACTIVO_ART` = 'S' and `ap`.`REF_PROVEEDOR_AP` is not null and `ap`.`REF_PROVEEDOR_AP` <> '';

-- Recreando vista: vi_caja_pagos
DROP VIEW IF EXISTS `vi_caja_pagos`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_caja_pagos` AS select `p`.`CODIGO_EMP_PAGO` AS `CODIGO_EMP_PAGO`,`p`.`CODIGO_ALM_PAGO` AS `CODIGO_ALM_PAGO`,`p`.`CODIGO_CAJA_PAGO` AS `CODIGO_CAJA_PAGO`,`p`.`SERIE_OPERACION_PAGO` AS `SERIE_OPERACION_PAGO`,`p`.`NUMERO_OPERACION_PAGO` AS `NUMERO_OPERACION_PAGO`,`p`.`NUMERO_LINEA_PAGO` AS `NUMERO_LINEA_PAGO`,`p`.`CODIGO_FP_CFP` AS `CODIGO_FP_CFP`,`p`.`CODIGO_DIVISA_PAGO` AS `CODIGO_DIVISA_PAGO`,`p`.`RED_BLOCKCHAIN_PAGO` AS `RED_BLOCKCHAIN_PAGO`,`p`.`FACTOR_CAMBIO_PAGO` AS `FACTOR_CAMBIO_PAGO`,`p`.`IMPORTE_DIVISA_PAGO` AS `IMPORTE_DIVISA_PAGO`,`p`.`IMPORTE_ENTREGADO_PAGO` AS `IMPORTE_ENTREGADO_PAGO`,`p`.`IMPORTE_CAMBIO_PAGO` AS `IMPORTE_CAMBIO_PAGO`,`p`.`REFERENCIA_FACPAG` AS `REFERENCIA_FACPAG`,`p`.`OBSERVACIONES_PAGO` AS `OBSERVACIONES_PAGO`,`p`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`p`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`p`.`USUARIO_ALTA` AS `USUARIO_ALTA`,coalesce(`o`.`FECHA_OP`,`p`.`INSTANTE_ALTA`) AS `FECHA_PAGO`,`o`.`NUM_FAC` AS `NUMERO_FAC_PAGO`,`o`.`SERIE_FAC` AS `SERIE_FAC_PAGO` from (`fza_caja_pagos` `p` left join (select `fza_caja_operaciones`.`CODIGO_EMP_OPCAJA` AS `CODIGO_EMP_OPCAJA`,`fza_caja_operaciones`.`CODIGO_ALM_OPCAJA` AS `CODIGO_ALM_OPCAJA`,`fza_caja_operaciones`.`CODIGO_CAJA_OPCAJA` AS `CODIGO_CAJA_OPCAJA`,`fza_caja_operaciones`.`NUMERO_OPERACION_OPCAJA` AS `NUMERO_OPERACION_OPCAJA`,min(`fza_caja_operaciones`.`FECHA_OPERACION_OPCAJA`) AS `FECHA_OP`,max(`fza_caja_operaciones`.`NUMERO_FAC_OPCAJA`) AS `NUM_FAC`,max(`fza_caja_operaciones`.`SERIE_FAC_OPCAJA`) AS `SERIE_FAC` from `fza_caja_operaciones` group by `fza_caja_operaciones`.`CODIGO_EMP_OPCAJA`,`fza_caja_operaciones`.`CODIGO_ALM_OPCAJA`,`fza_caja_operaciones`.`CODIGO_CAJA_OPCAJA`,`fza_caja_operaciones`.`NUMERO_OPERACION_OPCAJA`) `o` on(`o`.`CODIGO_EMP_OPCAJA` = `p`.`CODIGO_EMP_PAGO` and `o`.`CODIGO_ALM_OPCAJA` = `p`.`CODIGO_ALM_PAGO` and `o`.`CODIGO_CAJA_OPCAJA` = `p`.`CODIGO_CAJA_PAGO` and `o`.`NUMERO_OPERACION_OPCAJA` = `p`.`NUMERO_OPERACION_PAGO`));

-- Recreando vista: vi_caja_tarifa_sku_articulos
DROP VIEW IF EXISTS `vi_caja_tarifa_sku_articulos`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_caja_tarifa_sku_articulos` AS select `skus`.`CODIGO_UNIDAD_SKU` AS `CODIGO_UNIDAD_ARTTAR`,`skus`.`CODIGO_ART_SKU` AS `CODIGO_ART_ART`,`tarifas`.`CODIGO_TAR_ARTTAR` AS `CODIGO_TAR_ARTTAR`,`tarifas`.`NOMBRE_TAR_TAR` AS `NOMBRE_TAR_TAR`,coalesce(`tarifa_sku`.`PRECIO_FINAL_ARTTAR`,`tarifa_padre`.`PRECIO_FINAL_ARTTAR`) AS `PRECIO_FINAL_ARTTAR`,coalesce(`tarifa_sku`.`PRECIO_SALIDA_ARTTAR`,`tarifa_padre`.`PRECIO_SALIDA_ARTTAR`) AS `PRECIO_SALIDA_ARTTAR`,coalesce(`tarifa_sku`.`PORCENTAJE_DTO_ARTTAR`,`tarifa_padre`.`PORCENTAJE_DTO_ARTTAR`) AS `PORCENTAJE_DTO_ARTTAR`,coalesce(`tarifa_sku`.`PRECIO_DTO_ARTTAR`,`tarifa_padre`.`PRECIO_DTO_ARTTAR`) AS `PRECIO_DTO_ARTTAR`,case when `tarifa_sku`.`PRECIO_FINAL_ARTTAR` is not null then 'ESPECIFICO_SKU' else 'HEREDADO_PADRE' end AS `ORIGEN_PRECIO`,concat(`articulos`.`DESCRIPCION_ART`,' (',`skus`.`CODIGO_UNIDAD_SKU`,')') AS `DESCRIPCION_COMPLETA`,`tarifas`.`ESIMP_INCL_TAR` AS `ESIMP_INCL_TAR`,`articulos`.`TIPO_CANTIDAD_ART` AS `TIPO_CANTIDAD_ART`,`articulos`.`CODIGO_FAM_ART` AS `CODIGO_FAM_ART` from (((((`fza_articulos_skus` `skus` join `fza_articulos` `articulos` on(`skus`.`CODIGO_ART_SKU` = `articulos`.`CODIGO_ART_ART`)) join (select distinct `fza_articulos_tarifas`.`CODIGO_ART_ARTTAR` AS `CODIGO_ART_ARTTAR`,`fza_articulos_tarifas`.`CODIGO_TAR_ARTTAR` AS `CODIGO_TAR_ARTTAR` from `fza_articulos_tarifas`) `t_existentes` on(`t_existentes`.`CODIGO_ART_ARTTAR` = `articulos`.`CODIGO_ART_ART`)) join `fza_tarifas` `tarifas` on(`tarifas`.`CODIGO_TAR_ARTTAR` = `t_existentes`.`CODIGO_TAR_ARTTAR`)) left join `fza_articulos_tarifas` `tarifa_sku` on(`skus`.`CODIGO_UNIDAD_SKU` = `tarifa_sku`.`CODIGO_UNIDAD_ARTTAR` and `tarifas`.`CODIGO_TAR_ARTTAR` = `tarifa_sku`.`CODIGO_TAR_ARTTAR`)) left join `fza_articulos_tarifas` `tarifa_padre` on(`articulos`.`CODIGO_ART_ART` = `tarifa_padre`.`CODIGO_ART_ARTTAR` and `tarifas`.`CODIGO_TAR_ARTTAR` = `tarifa_padre`.`CODIGO_TAR_ARTTAR` and (`tarifa_padre`.`CODIGO_UNIDAD_ARTTAR` is null or `tarifa_padre`.`CODIGO_UNIDAD_ARTTAR` = ''))) where `skus`.`ESACTIVO_SKU` = 'S' and (`tarifa_sku`.`PRECIO_FINAL_ARTTAR` is not null or `tarifa_padre`.`PRECIO_FINAL_ARTTAR` is not null) order by `skus`.`CODIGO_ART_SKU`,`skus`.`CODIGO_UNIDAD_SKU`,`tarifas`.`ORDEN_TAR`;

-- Recreando vista: vi_caja_totalventas
DROP VIEW IF EXISTS `vi_caja_totalventas`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_caja_totalventas` AS select `fza_facturas`.`FECHA_FAC` AS `FECHA`,count(0) AS `TOTAL_VENTAS`,sum(`fza_facturas`.`TOTAL_LIQUIDO_FAC`) AS `TOTAL_COBRADO` from `fza_facturas` group by `fza_facturas`.`FECHA_FAC` order by `fza_facturas`.`FECHA_FAC`;

-- Recreando vista: vi_caja_vales_ptes
DROP VIEW IF EXISTS `vi_caja_vales_ptes`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_caja_vales_ptes` AS select `fza_caja_vales`.`CODIGO_VL` AS `codigo_vale`,`fza_caja_vales`.`PIN_SEGURIDAD_VL` AS `pin`,`fza_caja_vales`.`IMPORTE_NOMINAL_VL` AS `importe`,`fza_caja_vales`.`FECHA_EMISION_VL` AS `fecha_emision`,`fza_caja_vales`.`FECHA_CADUCIDAD_VL` AS `fecha_caducidad`,`fza_caja_vales`.`CODIGO_CAJA_EMI_VL` AS `caja`,`fza_caja_vales`.`CODIGO_ALM_EMI_VL` AS `almacen`,`fza_caja_vales`.`NUMERO_OPERACION_EMI_VL` AS `num_operacion`,`fza_caja_vales`.`SERIE_FAC_EMI_VL` AS `serie_factura`,`fza_caja_vales`.`NUMERO_FAC_EMI_VL` AS `num_factura`,`fza_caja_vales`.`CODIGO_CLI_VL` AS `cliente`,`fza_caja_vales`.`OBSERVACIONES_VL` AS `observaciones`,to_days(`fza_caja_vales`.`FECHA_CADUCIDAD_VL`) - to_days(curdate()) AS `dias_hasta_caducidad` from `fza_caja_vales` where `fza_caja_vales`.`ESTADO_VL` = 'PENDIENTE' and (`fza_caja_vales`.`FECHA_CADUCIDAD_VL` is null or `fza_caja_vales`.`FECHA_CADUCIDAD_VL` >= curdate()) order by `fza_caja_vales`.`FECHA_EMISION_VL` desc;

-- Recreando vista: vi_clientes
DROP VIEW IF EXISTS `vi_clientes`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_clientes` AS select `fza_clientes`.`CODIGO_CLI_CLI` AS `CODIGO_CLI_CLI`,`fza_clientes`.`ESACTIVO_CLI` AS `ESACTIVO_CLI`,`fza_clientes`.`ORDEN_CLI` AS `ORDEN_CLI`,`fza_clientes`.`RAZON_SOCIAL_CLI` AS `RAZON_SOCIAL_CLI`,`fza_clientes`.`NIF_CLI` AS `NIF_CLI`,`fza_clientes`.`MOVIL_CLI` AS `MOVIL_CLI`,`fza_clientes`.`EMAIL_CLI` AS `EMAIL_CLI`,`fza_clientes`.`DIRECCION1_CLI` AS `DIRECCION1_CLI`,`fza_clientes`.`DIRECCION2_CLI` AS `DIRECCION2_CLI`,`fza_clientes`.`POBLACION_CLI` AS `POBLACION_CLI`,`fza_clientes`.`PROVINCIA_CLI` AS `PROVINCIA_CLI`,`fza_clientes`.`CODIGO_POSTAL_CLI` AS `CODIGO_POSTAL_CLI`,`fza_clientes`.`CODIGO_PAI_CLI` AS `CODIGO_PAI_CLI`,`fza_clientes`.`NOMBRE_PAI_CLI` AS `NOMBRE_PAI_CLI`,`fza_clientes`.`OBSERVACIONES_CLI` AS `OBSERVACIONES_CLI`,`fza_clientes`.`REFERENCIA_CLI` AS `REFERENCIA_CLI`,`fza_clientes`.`CONTACTO_CLI` AS `CONTACTO_CLI`,`fza_clientes`.`TELEFONO_CONTACTO_CLI` AS `TELEFONO_CONTACTO_CLI`,`fza_clientes`.`TELEFONO_CLI` AS `TELEFONO_CLI`,`fza_clientes`.`IBAN_CLI` AS `IBAN_CLI`,`fza_clientes`.`ESIVA_RECARGO_CLI` AS `ESIVA_RECARGO_CLI`,`fza_clientes`.`ESRETENCIONES_CLI` AS `ESRETENCIONES_CLI`,`fza_clientes`.`TOTAL_LIMITE_CREDITO_CLI` AS `TOTAL_LIMITE_CREDITO_CLI`,`fza_clientes`.`ESPERMITE_DEUDA_CLI` AS `ESPERMITE_DEUDA_CLI`,`fza_clientes`.`TOTAL_DEUDA_CLI` AS `TOTAL_DEUDA_CLI`,`fza_clientes`.`ESIVA_EXENTO_CLI` AS `ESIVA_EXENTO_CLI`,`fza_clientes`.`ESINTRACOMUNITARIO_CLI` AS `ESINTRACOMUNITARIO_CLI`,`fza_clientes`.`ESREGIMENESPECIALAGRICOLA_CLI` AS `ESREGIMENESPECIALAGRICOLA_CLI`,`fza_clientes`.`CODIGO_FP_CLI` AS `CODIGO_FP_CLI`,`fza_clientes`.`TARIFA_ARTICULO_CLI` AS `TARIFA_ARTICULO_CLI`,`fza_clientes`.`SERIE_CON_CLI` AS `SERIE_CON_CLI`,`fza_clientes`.`TEXTO_LEGAL_FACTURA_CLI` AS `TEXTO_LEGAL_FACTURA_CLI`,`fza_clientes`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_clientes`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_clientes`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_clientes`.`USUARIO_MODIF` AS `USUARIO_MODIF` from `fza_clientes` order by `fza_clientes`.`ORDEN_CLI`;

-- Recreando vista: vi_cli_busquedas
DROP VIEW IF EXISTS `vi_cli_busquedas`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_cli_busquedas` AS select `fza_clientes`.`CODIGO_CLI_CLI` AS `CODIGO_CLI_CLI`,`fza_clientes`.`ESACTIVO_CLI` AS `ESACTIVO_CLI`,`fza_clientes`.`RAZON_SOCIAL_CLI` AS `RAZON_SOCIAL_CLI`,`fza_clientes`.`NIF_CLI` AS `NIF_CLI`,`fza_clientes`.`MOVIL_CLI` AS `MOVIL_CLI`,`fza_clientes`.`EMAIL_CLI` AS `EMAIL_CLI`,`fza_clientes`.`DIRECCION1_CLI` AS `DIRECCION1_CLI`,`fza_clientes`.`DIRECCION2_CLI` AS `DIRECCION2_CLI`,`fza_clientes`.`POBLACION_CLI` AS `POBLACION_CLI`,`fza_clientes`.`PROVINCIA_CLI` AS `PROVINCIA_CLI`,`fza_clientes`.`CODIGO_POSTAL_CLI` AS `CODIGO_POSTAL_CLI`,`fza_clientes`.`CODIGO_PAI_CLI` AS `CODIGO_PAI_CLI`,`fza_clientes`.`NOMBRE_PAI_CLI` AS `NOMBRE_PAI_CLI`,`fza_clientes`.`OBSERVACIONES_CLI` AS `OBSERVACIONES_CLI`,`fza_clientes`.`REFERENCIA_CLI` AS `REFERENCIA_CLI`,`fza_clientes`.`CONTACTO_CLI` AS `CONTACTO_CLI`,`fza_clientes`.`TELEFONO_CONTACTO_CLI` AS `TELEFONO_CONTACTO_CLI`,`fza_clientes`.`TELEFONO_CLI` AS `TELEFONO_CLI`,`fza_clientes`.`IBAN_CLI` AS `IBAN_CLI`,`fza_clientes`.`ESIVA_RECARGO_CLI` AS `ESIVA_RECARGO_CLI`,`fza_clientes`.`ESRETENCIONES_CLI` AS `ESRETENCIONES_CLI`,`fza_clientes`.`ESIVA_EXENTO_CLI` AS `ESIVA_EXENTO_CLI`,`fza_clientes`.`ESREGIMENESPECIALAGRICOLA_CLI` AS `ESREGIMENESPECIALAGRICOLA_CLI`,`fza_clientes`.`ESINTRACOMUNITARIO_CLI` AS `ESINTRACOMUNITARIO_CLI`,`fza_clientes`.`CODIGO_FP_CLI` AS `CODIGO_FP_CLI`,`fza_clientes`.`SERIE_CON_CLI` AS `SERIE_CON_CLI`,`fza_clientes`.`TARIFA_ARTICULO_CLI` AS `TARIFA_ARTICULO_CLI`,`fza_clientes`.`TEXTO_LEGAL_FACTURA_CLI` AS `TEXTO_LEGAL_FACTURA_CLI`,`fza_clientes`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_clientes`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_clientes`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_clientes`.`USUARIO_MODIF` AS `USUARIO_MODIF` from `fza_clientes` where `fza_clientes`.`ESACTIVO_CLI` = 'S' order by `fza_clientes`.`CODIGO_CLI_CLI` desc;

-- Recreando vista: vi_compras_sesiones
DROP VIEW IF EXISTS `vi_compras_sesiones`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_compras_sesiones` AS select `s`.`SERIE_SES` AS `SERIE_SES`,`s`.`NUMERO_SES` AS `NUMERO_SES`,`s`.`FECHA_SES` AS `FECHA_SES`,`s`.`ESTADO_SES` AS `ESTADO_SES`,`s`.`CODIGO_EMP_SES` AS `CODIGO_EMP_SES`,`s`.`CODIGO_PRV_SES` AS `CODIGO_PRV_SES`,`s`.`REF_PRV_SES` AS `REF_PRV_SES`,`s`.`CODIGO_FAM_SES` AS `CODIGO_FAM_SES`,`s`.`CODIGO_ALM_SES` AS `CODIGO_ALM_SES`,`s`.`MONEDA_SES` AS `MONEDA_SES`,`s`.`TIPO_IVA_SES` AS `TIPO_IVA_SES`,`s`.`PORCENTAJE_MARGEN_SES` AS `PORCENTAJE_MARGEN_SES`,`s`.`CODIGO_TAR_SES` AS `CODIGO_TAR_SES`,`s`.`ESPRECIOS_SIN_IVA_SES` AS `ESPRECIOS_SIN_IVA_SES`,`s`.`ESREDONDEO_VENTA_SES` AS `ESREDONDEO_VENTA_SES`,`s`.`MULTIPLO_REDONDEO_SES` AS `MULTIPLO_REDONDEO_SES`,`s`.`AJUSTE_FINAL_SES` AS `AJUSTE_FINAL_SES`,`s`.`CODIGO_VAR_SES` AS `CODIGO_VAR_SES`,`s`.`ID_VA_PIVOT_SES` AS `ID_VA_PIVOT_SES`,`s`.`ID_AC_PIVOT_SES` AS `ID_AC_PIVOT_SES`,`s`.`ID_VA_FILA_SES` AS `ID_VA_FILA_SES`,`s`.`ID_AC_FILA_SES` AS `ID_AC_FILA_SES`,`s`.`ESVAR_FIJA_SES` AS `ESVAR_FIJA_SES`,`s`.`PREFIJO_EAN_SES` AS `PREFIJO_EAN_SES`,`s`.`INSTANTE_MATERIALIZA_SES` AS `INSTANTE_MATERIALIZA_SES`,`s`.`USUARIO_MATERIALIZA_SES` AS `USUARIO_MATERIALIZA_SES`,`s`.`ESGENERA_PEDIDO_SES` AS `ESGENERA_PEDIDO_SES`,`s`.`ESGENERA_ALBARAN_SES` AS `ESGENERA_ALBARAN_SES`,`s`.`ESFORMATO_DISTRIBUIDO_SES` AS `ESFORMATO_DISTRIBUIDO_SES`,`s`.`SERIE_PEDC_SES` AS `SERIE_PEDC_SES`,`s`.`NUMERO_PEDC_SES` AS `NUMERO_PEDC_SES`,`s`.`SERIE_ALBC_SES` AS `SERIE_ALBC_SES`,`s`.`NUMERO_ALBC_SES` AS `NUMERO_ALBC_SES`,`s`.`MENSAJE_ERROR_SES` AS `MENSAJE_ERROR_SES`,`s`.`CONTADOR_LINEAS_SES` AS `CONTADOR_LINEAS_SES`,`s`.`COMENTARIOS_SES` AS `COMENTARIOS_SES`,`s`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`s`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`s`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`s`.`USUARIO_MODIF` AS `USUARIO_MODIF`,`s`.`ESPRECIO_POR_SKU_SES` AS `ESPRECIO_POR_SKU_SES`,`s`.`ID_PV_TEMPORADA_SES` AS `ID_PV_TEMPORADA_SES`,`prv`.`RAZON_SOCIAL_PRV` AS `RAZON_SOCIAL_PRV_SES`,`prv`.`NOMBRE_PRV` AS `NOMBRE_PRV_SES` from (`fza_compras_sesiones` `s` left join `fza_proveedores` `prv` on(`prv`.`CODIGO_PRV_PRV` = `s`.`CODIGO_PRV_SES`));

-- Recreando vista: vi_compras_sesiones_cab_print
DROP VIEW IF EXISTS `vi_compras_sesiones_cab_print`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_compras_sesiones_cab_print` AS select `ses`.`SERIE_SES` AS `SERIE_SES`,`ses`.`NUMERO_SES` AS `NUMERO_SES`,`ses`.`FECHA_SES` AS `FECHA_SES`,`ses`.`ESTADO_SES` AS `ESTADO_SES`,`ses`.`REF_PRV_SES` AS `REF_PRV_SES`,`ses`.`COMENTARIOS_SES` AS `COMENTARIOS_SES`,`ses`.`PORCENTAJE_MARGEN_SES` AS `PORCENTAJE_MARGEN_SES`,`ses`.`MULTIPLO_REDONDEO_SES` AS `MULTIPLO_REDONDEO_SES`,`ses`.`AJUSTE_FINAL_SES` AS `AJUSTE_FINAL_SES`,`ses`.`MONEDA_SES` AS `MONEDA_SES`,`ses`.`TIPO_IVA_SES` AS `TIPO_IVA_SES`,`ses`.`ESFORMATO_DISTRIBUIDO_SES` AS `ESFORMATO_DISTRIBUIDO_SES`,`ses`.`CODIGO_EMP_SES` AS `CODIGO_EMP_SES`,`emp`.`RAZON_SOCIAL_EMP` AS `RAZON_SOCIAL_EMP`,`emp`.`DIRECCION1_EMP` AS `DIRECCION1_EMP`,`emp`.`CODIGO_POSTAL_EMP` AS `CODIGO_POSTAL_EMP`,`emp`.`POBLACION_EMP` AS `POBLACION_EMP`,`emp`.`PROVINCIA_EMP` AS `PROVINCIA_EMP`,`emp`.`NIF_EMP` AS `CIF_EMP`,`emp`.`MOVIL_EMP` AS `TELEFONO1_EMP`,`ses`.`CODIGO_PRV_SES` AS `CODIGO_PRV_SES`,`prv`.`RAZON_SOCIAL_PRV` AS `RAZON_SOCIAL_PRV`,`prv`.`DIRECCION1_PRV` AS `DIRECCION1_PRV`,`prv`.`CODIGO_POSTAL_PRV` AS `CODIGO_POSTAL_PRV`,`prv`.`POBLACION_PRV` AS `POBLACION_PRV`,`prv`.`PROVINCIA_PRV` AS `PROVINCIA_PRV`,`prv`.`NIF_PRV` AS `CIF_PRV`,coalesce(`prv`.`TELEFONO_PRV`,`prv`.`MOVIL_PRV`) AS `TELEFONO1_PRV`,`ses`.`CODIGO_TAR_SES` AS `CODIGO_TAR_SES`,`ses`.`CODIGO_FAM_SES` AS `CODIGO_FAM_SES`,`ses`.`CODIGO_ALM_SES` AS `CODIGO_ALM_SES`,`ses`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`ses`.`USUARIO_ALTA` AS `USUARIO_ALTA`,(select coalesce(sum(`lin`.`TOTAL_UNIDADES_SESLIN`),0) from `fza_compras_sesiones_lineas` `lin` where `lin`.`SERIE_SES_SESLIN` = `ses`.`SERIE_SES` and `lin`.`NUMERO_SES_SESLIN` = `ses`.`NUMERO_SES`) AS `TOTAL_UNIDADES_SES`,(select coalesce(sum(`lin`.`TOTAL_LINEA_SESLIN`),0) from `fza_compras_sesiones_lineas` `lin` where `lin`.`SERIE_SES_SESLIN` = `ses`.`SERIE_SES` and `lin`.`NUMERO_SES_SESLIN` = `ses`.`NUMERO_SES`) AS `TOTAL_LINEAS_SES`,(select count(0) from `fza_compras_sesiones_lineas` `lin` where `lin`.`SERIE_SES_SESLIN` = `ses`.`SERIE_SES` and `lin`.`NUMERO_SES_SESLIN` = `ses`.`NUMERO_SES`) AS `NUM_LINEAS_SES` from ((`fza_compras_sesiones` `ses` left join `fza_empresas` `emp` on(`emp`.`CODIGO_EMP_EMP` = `ses`.`CODIGO_EMP_SES`)) left join `fza_proveedores` `prv` on(`prv`.`CODIGO_PRV_PRV` = `ses`.`CODIGO_PRV_SES`));

-- Recreando vista: vi_compras_sesiones_guias_print
DROP VIEW IF EXISTS `vi_compras_sesiones_guias_print`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_compras_sesiones_guias_print` AS with pos_acd as (select `acd`.`ID_AC_ACD` AS `ID_AC`,`acd`.`ID_AV_ACD` AS `ID_AV`,`av`.`AV` AS `AV`,row_number() over ( partition by `acd`.`ID_AC_ACD` order by `acd`.`ORDEN_ACD`,`acd`.`ID_AV_ACD`) AS `POSICION` from (`fza_atributos_conjuntos_det` `acd` join `fza_atributos_valores` `av` on(`av`.`ID_AV` = `acd`.`ID_AV_ACD`)))select `ac`.`ID_AC` AS `ID_AC`,`ac`.`NOMBRE_AC` AS `NOMBRE_AC`,coalesce(`ac`.`NOMBRE_CORTO_AC`,ucase(left(`ac`.`NOMBRE_AC`,8))) AS `NOMBRE_CORTO_AC`,max(case when `p`.`POSICION` = 1 then `p`.`AV` end) AS `T01`,max(case when `p`.`POSICION` = 2 then `p`.`AV` end) AS `T02`,max(case when `p`.`POSICION` = 3 then `p`.`AV` end) AS `T03`,max(case when `p`.`POSICION` = 4 then `p`.`AV` end) AS `T04`,max(case when `p`.`POSICION` = 5 then `p`.`AV` end) AS `T05`,max(case when `p`.`POSICION` = 6 then `p`.`AV` end) AS `T06`,max(case when `p`.`POSICION` = 7 then `p`.`AV` end) AS `T07`,max(case when `p`.`POSICION` = 8 then `p`.`AV` end) AS `T08`,max(case when `p`.`POSICION` = 9 then `p`.`AV` end) AS `T09`,max(case when `p`.`POSICION` = 10 then `p`.`AV` end) AS `T10`,max(case when `p`.`POSICION` = 11 then `p`.`AV` end) AS `T11`,max(case when `p`.`POSICION` = 12 then `p`.`AV` end) AS `T12`,max(case when `p`.`POSICION` = 13 then `p`.`AV` end) AS `T13`,max(case when `p`.`POSICION` = 14 then `p`.`AV` end) AS `T14`,max(case when `p`.`POSICION` = 15 then `p`.`AV` end) AS `T15`,max(case when `p`.`POSICION` = 16 then `p`.`AV` end) AS `T16`,max(case when `p`.`POSICION` = 17 then `p`.`AV` end) AS `T17`,max(case when `p`.`POSICION` = 18 then `p`.`AV` end) AS `T18`,max(case when `p`.`POSICION` = 19 then `p`.`AV` end) AS `T19`,max(case when `p`.`POSICION` = 20 then `p`.`AV` end) AS `T20` from (`fza_atributos_conjuntos` `ac` join `pos_acd` `p` on(`p`.`ID_AC` = `ac`.`ID_AC`)) where `p`.`POSICION` <= 20 and `ac`.`ESACTIVO_AC` = 'S' group by `ac`.`ID_AC`,`ac`.`NOMBRE_AC`,`ac`.`NOMBRE_CORTO_AC`;

-- Recreando vista: vi_compras_sesiones_lin_print
DROP VIEW IF EXISTS `vi_compras_sesiones_lin_print`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_compras_sesiones_lin_print` AS with pos_acd as (select `fza_atributos_conjuntos_det`.`ID_AC_ACD` AS `ID_AC`,`fza_atributos_conjuntos_det`.`ID_AV_ACD` AS `ID_AV`,row_number() over ( partition by `fza_atributos_conjuntos_det`.`ID_AC_ACD` order by `fza_atributos_conjuntos_det`.`ORDEN_ACD`,`fza_atributos_conjuntos_det`.`ID_AV_ACD`) AS `POSICION` from `fza_atributos_conjuntos_det`)select `lin`.`SERIE_SES_SESLIN` AS `SERIE_SES`,`lin`.`NUMERO_SES_SESLIN` AS `NUMERO_SES`,`lin`.`LINEA_SESLIN` AS `LINEA_SES`,`lin`.`CODIGO_ART_TENTATIVO_SESLIN` AS `CODIGO_ART`,`lin`.`REF_PRV_SESLIN` AS `REF_PRV`,`lin`.`DESCRIPCION_SESLIN` AS `DESCRIPCION`,`lin`.`COLOR_TEXTO_SESLIN` AS `COLOR_TEXTO`,`lin`.`CODIGO_ATB_COLOR_SESLIN` AS `CODIGO_ATB_COLOR`,`lin`.`PRECIO_COMPRA_SESLIN` AS `PRECIO_COMPRA`,`lin`.`PRECIO_VENTA_SESLIN` AS `PRECIO_VENTA`,`lin`.`ID_AC_PIVOT_SESLIN` AS `ID_AC_PIVOT`,`ac`.`NOMBRE_AC` AS `NOMBRE_AC`,coalesce(`ac`.`NOMBRE_CORTO_AC`,ucase(left(`ac`.`NOMBRE_AC`,8))) AS `NOMBRE_CORTO_AC`,ifnull(nullif(`cel`.`CODIGO_ALM_SESCEL`,''),`ses`.`CODIGO_ALM_SES`) AS `CODIGO_ALM`,coalesce(`alm`.`NOMBRE_ALM_ALM`,ifnull(nullif(`cel`.`CODIGO_ALM_SESCEL`,''),`ses`.`CODIGO_ALM_SES`)) AS `NOMBRE_ALM`,sum(ifnull(`cel`.`CANTIDAD_SESCEL`,0)) AS `TOTAL_UNIDADES`,sum(ifnull(`cel`.`CANTIDAD_SESCEL`,0)) * `lin`.`PRECIO_COMPRA_SESLIN` AS `TOTAL_LINEA`,coalesce(sum(case when `p`.`POSICION` = 1 then `cel`.`CANTIDAD_SESCEL` end),0) AS `T01`,coalesce(sum(case when `p`.`POSICION` = 2 then `cel`.`CANTIDAD_SESCEL` end),0) AS `T02`,coalesce(sum(case when `p`.`POSICION` = 3 then `cel`.`CANTIDAD_SESCEL` end),0) AS `T03`,coalesce(sum(case when `p`.`POSICION` = 4 then `cel`.`CANTIDAD_SESCEL` end),0) AS `T04`,coalesce(sum(case when `p`.`POSICION` = 5 then `cel`.`CANTIDAD_SESCEL` end),0) AS `T05`,coalesce(sum(case when `p`.`POSICION` = 6 then `cel`.`CANTIDAD_SESCEL` end),0) AS `T06`,coalesce(sum(case when `p`.`POSICION` = 7 then `cel`.`CANTIDAD_SESCEL` end),0) AS `T07`,coalesce(sum(case when `p`.`POSICION` = 8 then `cel`.`CANTIDAD_SESCEL` end),0) AS `T08`,coalesce(sum(case when `p`.`POSICION` = 9 then `cel`.`CANTIDAD_SESCEL` end),0) AS `T09`,coalesce(sum(case when `p`.`POSICION` = 10 then `cel`.`CANTIDAD_SESCEL` end),0) AS `T10`,coalesce(sum(case when `p`.`POSICION` = 11 then `cel`.`CANTIDAD_SESCEL` end),0) AS `T11`,coalesce(sum(case when `p`.`POSICION` = 12 then `cel`.`CANTIDAD_SESCEL` end),0) AS `T12`,coalesce(sum(case when `p`.`POSICION` = 13 then `cel`.`CANTIDAD_SESCEL` end),0) AS `T13`,coalesce(sum(case when `p`.`POSICION` = 14 then `cel`.`CANTIDAD_SESCEL` end),0) AS `T14`,coalesce(sum(case when `p`.`POSICION` = 15 then `cel`.`CANTIDAD_SESCEL` end),0) AS `T15`,coalesce(sum(case when `p`.`POSICION` = 16 then `cel`.`CANTIDAD_SESCEL` end),0) AS `T16`,coalesce(sum(case when `p`.`POSICION` = 17 then `cel`.`CANTIDAD_SESCEL` end),0) AS `T17`,coalesce(sum(case when `p`.`POSICION` = 18 then `cel`.`CANTIDAD_SESCEL` end),0) AS `T18`,coalesce(sum(case when `p`.`POSICION` = 19 then `cel`.`CANTIDAD_SESCEL` end),0) AS `T19`,coalesce(sum(case when `p`.`POSICION` = 20 then `cel`.`CANTIDAD_SESCEL` end),0) AS `T20` from (((((`fza_compras_sesiones_lineas` `lin` join `fza_compras_sesiones` `ses` on(`ses`.`SERIE_SES` = `lin`.`SERIE_SES_SESLIN` and `ses`.`NUMERO_SES` = `lin`.`NUMERO_SES_SESLIN`)) left join `fza_atributos_conjuntos` `ac` on(`ac`.`ID_AC` = `lin`.`ID_AC_PIVOT_SESLIN`)) left join `fza_compras_sesiones_celdas` `cel` on(`cel`.`SERIE_SES_SESCEL` = `lin`.`SERIE_SES_SESLIN` and `cel`.`NUMERO_SES_SESCEL` = `lin`.`NUMERO_SES_SESLIN` and `cel`.`LINEA_SES_SESCEL` = `lin`.`LINEA_SESLIN`)) left join `pos_acd` `p` on(`p`.`ID_AC` = `lin`.`ID_AC_PIVOT_SESLIN` and `p`.`ID_AV` = `cel`.`ID_AV_PIVOT_SESCEL`)) left join `fza_almacenes` `alm` on(`alm`.`CODIGO_ALM_ALM` = ifnull(nullif(`cel`.`CODIGO_ALM_SESCEL`,''),`ses`.`CODIGO_ALM_SES`))) group by `lin`.`SERIE_SES_SESLIN`,`lin`.`NUMERO_SES_SESLIN`,`lin`.`LINEA_SESLIN`,`lin`.`CODIGO_ART_TENTATIVO_SESLIN`,`lin`.`REF_PRV_SESLIN`,`lin`.`DESCRIPCION_SESLIN`,`lin`.`COLOR_TEXTO_SESLIN`,`lin`.`CODIGO_ATB_COLOR_SESLIN`,`lin`.`PRECIO_COMPRA_SESLIN`,`lin`.`PRECIO_VENTA_SESLIN`,`lin`.`ID_AC_PIVOT_SESLIN`,`ac`.`NOMBRE_AC`,`ac`.`NOMBRE_CORTO_AC`,ifnull(nullif(`cel`.`CODIGO_ALM_SESCEL`,''),`ses`.`CODIGO_ALM_SES`),`alm`.`NOMBRE_ALM_ALM`;

-- Recreando vista: vi_contadores
DROP VIEW IF EXISTS `vi_contadores`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_contadores` AS select `fza_contadores`.`TIPO_DOC_CON` AS `TIPO_DOC_CON`,`fza_contadores`.`SERIE_CON` AS `SERIE_CON`,`fza_contadores`.`CON` AS `CON`,`fza_contadores`.`EMPRESA_CON` AS `EMPRESA_CON`,`fza_tipos_documentos`.`DESCRIPCION_TIPO_DOCUMENTO_TD` AS `DESCRIPCION_TIPO_DOCUMENTO_TD`,`fza_tipos_documentos`.`TABLA_ORIGEN_TIPO_DOCUMENTO_TD` AS `TABLA_ORIGEN_TIPO_DOCUMENTO_TD`,`fza_contadores`.`DEFAULT_CON` AS `DEFAULT_CON`,`fza_contadores`.`NUM_DIGITOS_CON` AS `NUM_DIGITOS_CON`,`fza_contadores`.`ESACTIVO_CON` AS `ESACTIVO_CON`,`fza_contadores`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_contadores`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_contadores`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_contadores`.`USUARIO_MODIF` AS `USUARIO_MODIF` from (`fza_contadores` left join `fza_tipos_documentos` on(`fza_contadores`.`TIPO_DOC_CON` = `fza_tipos_documentos`.`CODIGO_TIPO_DOCUMENTO_TD`));

-- Recreando vista: vi_depositos_cliente
DROP VIEW IF EXISTS `vi_depositos_cliente`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_depositos_cliente` AS select `dc`.`ID_DEPOSITO_DEP` AS `ID_DEPOSITO_DEP`,`dc`.`CODIGO_EMP_DEP` AS `CODIGO_EMP_DEP`,`dc`.`CODIGO_CLI_DEP` AS `CODIGO_CLI_DEP`,`dc`.`CODIGO_ART_DEP` AS `CODIGO_ART_DEP`,`dc`.`CODIGO_UNIDAD_DEP` AS `CODIGO_UNIDAD_DEP`,`dc`.`PRECIO_VENTA_DEP` AS `PRECIO_VENTA_DEP`,`dc`.`IMPORTE_ANTICIPO_DEP` AS `IMPORTE_ANTICIPO_DEP`,`dc`.`ESTADO_DEP` AS `ESTADO_DEP`,`dc`.`FECHA_CREACION_DEP` AS `FECHA_CREACION_DEP`,`dc`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`dc`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`dc`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`dc`.`USUARIO_MODIF` AS `USUARIO_MODIF`,`dc`.`TIPO_IVA_DEP` AS `TIPO_IVA_DEP`,`dc`.`PORCENTAJE_IVA_DEP` AS `PORCENTAJE_IVA_DEP`,`dc`.`ESIMP_INCL_DEP` AS `ESIMP_INCL_DEP`,`dc`.`CANTIDAD_PENDIENTE_DEP` AS `CANTIDAD_PENDIENTE_DEP`,`dc`.`FECHA_ENTREGA_DEP` AS `FECHA_ENTREGA_DEP`,`a`.`DESCRIPCION_ART` AS `DESCRIPCION_ART` from (`fza_depositos_cliente` `dc` left join `fza_articulos` `a` on(`dc`.`CODIGO_ART_DEP` = `a`.`CODIGO_ART_ART`));

-- Recreando vista: vi_empleados
DROP VIEW IF EXISTS `vi_empleados`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_empleados` AS select `fza_empleados`.`CODIGO_EMPL` AS `CODIGO_EMPL`,`fza_empleados`.`NOMBRE_EMPL` AS `NOMBRE_EMPL`,`fza_empleados`.`DIRECCION_EMPL` AS `DIRECCION_EMPL`,`fza_empleados`.`TELEFONO_EMPL` AS `TELEFONO_EMPL`,`fza_empleados`.`DIMINUTIVO_TICKET_EMPL` AS `DIMINUTIVO_TICKET_EMPL`,`fza_empleados`.`ESACTIVO_EMPL` AS `ESACTIVO_EMPL`,`fza_empleados`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_empleados`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_empleados`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_empleados`.`USUARIO_MODIF` AS `USUARIO_MODIF` from `fza_empleados`;

-- Recreando vista: vi_empresas
DROP VIEW IF EXISTS `vi_empresas`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_empresas` AS select `fza_empresas`.`CODIGO_EMP_EMP` AS `CODIGO_EMP_EMP`,`fza_empresas`.`ORDEN_EMP` AS `ORDEN_EMP`,`fza_empresas`.`ESACTIVO_EMP` AS `ESACTIVO_EMP`,`fza_empresas`.`RAZON_SOCIAL_EMP` AS `RAZON_SOCIAL_EMP`,`fza_empresas`.`NIF_EMP` AS `NIF_EMP`,`fza_empresas`.`MOVIL_EMP` AS `MOVIL_EMP`,`fza_empresas`.`EMAIL_EMP` AS `EMAIL_EMP`,`fza_empresas`.`DIRECCION1_EMP` AS `DIRECCION1_EMP`,`fza_empresas`.`DIRECCION2_EMP` AS `DIRECCION2_EMP`,`fza_empresas`.`CODIGO_POSTAL_EMP` AS `CODIGO_POSTAL_EMP`,`fza_empresas`.`POBLACION_EMP` AS `POBLACION_EMP`,`fza_empresas`.`PROVINCIA_EMP` AS `PROVINCIA_EMP`,`fza_empresas`.`NOMBRE_PAI_EMP` AS `NOMBRE_PAI_EMP`,`fza_empresas`.`CODIGO_PAI_EMP` AS `CODIGO_PAI_EMP`,`fza_empresas`.`IBAN_EMP` AS `IBAN_EMP`,`fza_empresas`.`GRUPO_ZONA_IVA_EMP` AS `GRUPO_ZONA_IVA_EMP`,`fza_ivas_grupos`.`DESCRIPCION_IVA_IVAGRP` AS `DESCRIPCION_IVA_IVAGRP`,`fza_empresas`.`ESRETENCIONES_EMP` AS `ESRETENCIONES_EMP`,`fza_empresas`.`ESREGIMENESPECIALAGRICOLA_EMP` AS `ESREGIMENESPECIALAGRICOLA_EMP`,`fza_empresas`.`TEXTO_LEGAL_FACTURA_EMP` AS `TEXTO_LEGAL_FACTURA_EMP`,`fza_empresas`.`CODIGO_CERTIFICADO_EMP` AS `CODIGO_CERTIFICADO_EMP`,`fza_empresas`.`TITULAR_CERTIFICADO_EMP` AS `TITULAR_CERTIFICADO_EMP`,`fza_empresas`.`TIPO_CERTIFICADO_EMP` AS `TIPO_CERTIFICADO_EMP`,`fza_empresas`.`FECHA_DESDE_CERTIFICADO_EMP` AS `FECHA_DESDE_CERTIFICADO_EMP`,`fza_empresas`.`FECHA_HASTA_CERTIFICADO_EMP` AS `FECHA_HASTA_CERTIFICADO_EMP`,`fza_empresas`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_empresas`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_empresas`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_empresas`.`USUARIO_MODIF` AS `USUARIO_MODIF` from (`fza_empresas` left join `fza_ivas_grupos` on(`fza_empresas`.`GRUPO_ZONA_IVA_EMP` = `fza_ivas_grupos`.`IVA_IVAGRP`));

-- Recreando vista: vi_empresas_retenciones
DROP VIEW IF EXISTS `vi_empresas_retenciones`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_empresas_retenciones` AS select `fza_empresas_retenciones`.`CODIGO_RETENCION_EMPRET` AS `CODIGO_RETENCION_EMPRET`,`fza_empresas_retenciones`.`CODIGO_EMP_EMPRET` AS `CODIGO_EMP_EMPRET`,`fza_empresas_retenciones`.`PORCENTAJE_EMPRET` AS `PORCENTAJE_EMPRET`,`fza_empresas_retenciones`.`FECHA_DESDE_EMPRET` AS `FECHA_DESDE_EMPRET`,`fza_empresas_retenciones`.`FECHA_HASTA_EMPRET` AS `FECHA_HASTA_EMPRET`,`fza_empresas_retenciones`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_empresas_retenciones`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_empresas_retenciones`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_empresas_retenciones`.`USUARIO_MODIF` AS `USUARIO_MODIF` from `fza_empresas_retenciones`;

-- Recreando vista: vi_empresas_series
DROP VIEW IF EXISTS `vi_empresas_series`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_empresas_series` AS select `fza_empresas_series`.`CODIGO_SERIE_EMPSER` AS `CODIGO_SERIE_EMPSER`,`fza_empresas_series`.`CODIGO_EMP_EMPSER` AS `CODIGO_EMP_EMPSER`,`fza_empresas_series`.`CODIGO_ALM_EMPSER` AS `CODIGO_ALM_EMPSER`,`fza_empresas_series`.`CODIGO_CAJA_EMPSER` AS `CODIGO_CAJA_EMPSER`,`fza_empresas_series`.`EMPSER` AS `EMPSER`,`fza_empresas_series`.`TIPO_DOC_EMPSER` AS `TIPO_DOC_EMPSER`,`fza_empresas_series`.`SUBTIPO_EMPSER` AS `SUBTIPO_EMPSER`,`fza_empresas_series`.`FECHA_DESDE_EMPSER` AS `FECHA_DESDE_EMPSER`,`fza_empresas_series`.`FECHA_HASTA_EMPSER` AS `FECHA_HASTA_EMPSER`,`fza_empresas_series`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_empresas_series`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_empresas_series`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_empresas_series`.`USUARIO_MODIF` AS `USUARIO_MODIF` from `fza_empresas_series`;

-- Recreando vista: vi_emp_busquedas
DROP VIEW IF EXISTS `vi_emp_busquedas`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_emp_busquedas` AS select `fza_empresas`.`CODIGO_EMP_EMP` AS `CODIGO_EMP_EMP`,`fza_empresas`.`RAZON_SOCIAL_EMP` AS `RAZON_SOCIAL_EMP`,`fza_empresas`.`NIF_EMP` AS `NIF_EMP`,`fza_empresas`.`MOVIL_EMP` AS `MOVIL_EMP`,`fza_empresas`.`EMAIL_EMP` AS `EMAIL_EMP`,`fza_empresas`.`DIRECCION1_EMP` AS `DIRECCION1_EMP`,`fza_empresas`.`DIRECCION2_EMP` AS `DIRECCION2_EMP`,`fza_empresas`.`CODIGO_POSTAL_EMP` AS `CODIGO_POSTAL_EMP`,`fza_empresas`.`POBLACION_EMP` AS `POBLACION_EMP`,`fza_empresas`.`PROVINCIA_EMP` AS `PROVINCIA_EMP`,`fza_empresas`.`CODIGO_PAI_EMP` AS `CODIGO_PAI_EMP`,`fza_empresas`.`NOMBRE_PAI_EMP` AS `NOMBRE_PAI_EMP`,`fza_empresas`.`SERIE_CON_EMP` AS `SERIE_CON_EMP`,`fza_empresas`.`GRUPO_ZONA_IVA_EMP` AS `GRUPO_ZONA_IVA_EMP`,`fza_empresas`.`ESRETENCIONES_EMP` AS `ESRETENCIONES_EMP`,`fza_empresas`.`ESREGIMENESPECIALAGRICOLA_EMP` AS `ESREGIMENESPECIALAGRICOLA_EMP`,`fza_empresas`.`TEXTO_LEGAL_FACTURA_EMP` AS `TEXTO_LEGAL_FACTURA_EMP` from `fza_empresas` where `fza_empresas`.`ESACTIVO_EMP` = 'S' order by `fza_empresas`.`ORDEN_EMP`;

-- Recreando vista: vi_facturas
DROP VIEW IF EXISTS `vi_facturas`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_facturas` AS select `fza_facturas`.`FECHA_FAC` AS `FECHA_FAC`,`fza_facturas`.`NUMERO_FAC` AS `NUMERO_FAC`,`fza_facturas`.`SERIE_FAC` AS `SERIE_FAC`,`fza_facturas`.`TIPO_FAC` AS `TIPO_FAC`,`fza_facturas`.`FASE_FAC` AS `FASE_FAC`,`fza_facturas`.`TOTAL_LIQUIDO_FAC` AS `TOTAL_LIQUIDO_FAC`,`fza_facturas`.`PORCENTAJE_RETENCION_FAC` AS `PORCENTAJE_RETENCION_FAC`,`fza_facturas`.`TOTAL_RETENCION_FAC` AS `TOTAL_RETENCION_FAC`,`fza_facturas`.`TOTAL_IMPUESTOS_FAC` AS `TOTAL_IMPUESTOS_FAC`,`fza_facturas`.`TOTAL_BASES_FAC` AS `TOTAL_BASES_FAC`,`fza_facturas`.`CODIGO_CAJERO_FAC` AS `CODIGO_CAJERO_FAC`,`fza_facturas`.`FORMA_PAGO_FAC` AS `FORMA_PAGO_FAC`,`fza_facturas`.`CODIGO_EMP_FAC` AS `CODIGO_EMP_FAC`,`fza_facturas`.`RAZON_SOCIAL_EMPRESA_FAC` AS `RAZON_SOCIAL_EMPRESA_FAC`,`fza_facturas`.`NIF_EMPRESA_FAC` AS `NIF_EMPRESA_FAC`,`fza_facturas`.`MOVIL_EMPRESA_FAC` AS `MOVIL_EMPRESA_FAC`,`fza_facturas`.`EMAIL_EMPRESA_FAC` AS `EMAIL_EMPRESA_FAC`,`fza_facturas`.`DIRECCION1_EMPRESA_FAC` AS `DIRECCION1_EMPRESA_FAC`,`fza_facturas`.`DIRECCION2_EMPRESA_FAC` AS `DIRECCION2_EMPRESA_FAC`,`fza_facturas`.`POBLACION_EMPRESA_FAC` AS `POBLACION_EMPRESA_FAC`,`fza_facturas`.`PROVINCIA_EMPRESA_FAC` AS `PROVINCIA_EMPRESA_FAC`,`fza_facturas`.`NOMBRE_PAI_EMPRESA_FAC` AS `NOMBRE_PAI_EMPRESA_FAC`,`fza_facturas`.`CODIGO_PAI_EMPRESA_FAC` AS `CODIGO_PAI_EMPRESA_FAC`,`fza_facturas`.`CODIGO_POSTAL_EMPRESA_FAC` AS `CODIGO_POSTAL_EMPRESA_FAC`,`fza_facturas`.`ESRETENCIONES_EMPRESA_FAC` AS `ESRETENCIONES_EMPRESA_FAC`,`fza_facturas`.`GRUPO_ZONA_IVA_EMPRESA_FAC` AS `GRUPO_ZONA_IVA_EMPRESA_FAC`,`fza_facturas`.`ESREGIMENESPECIALAGRICOLA_EMPRESA_FAC` AS `ESREGIMENESPECIALAGRICOLA_EMPRESA_FAC`,`fza_facturas`.`CODIGO_CLI_FAC` AS `CODIGO_CLI_FAC`,`fza_facturas`.`RAZON_SOCIAL_CLIENTE_FAC` AS `RAZON_SOCIAL_CLIENTE_FAC`,`fza_facturas`.`NIF_CLIENTE_FAC` AS `NIF_CLIENTE_FAC`,`fza_facturas`.`MOVIL_CLIENTE_FAC` AS `MOVIL_CLIENTE_FAC`,`fza_facturas`.`EMAIL_CLIENTE_FAC` AS `EMAIL_CLIENTE_FAC`,`fza_facturas`.`DIRECCION1_CLIENTE_FAC` AS `DIRECCION1_CLIENTE_FAC`,`fza_facturas`.`DIRECCION2_CLIENTE_FAC` AS `DIRECCION2_CLIENTE_FAC`,`fza_facturas`.`POBLACION_CLIENTE_FAC` AS `POBLACION_CLIENTE_FAC`,`fza_facturas`.`PROVINCIA_CLIENTE_FAC` AS `PROVINCIA_CLIENTE_FAC`,`fza_facturas`.`CODIGO_POSTAL_CLIENTE_FAC` AS `CODIGO_POSTAL_CLIENTE_FAC`,`fza_facturas`.`NOMBRE_PAI_CLIENTE_FAC` AS `NOMBRE_PAI_CLIENTE_FAC`,`fza_facturas`.`CODIGO_PAI_CLIENTE_FAC` AS `CODIGO_PAI_CLIENTE_FAC`,`fza_facturas`.`ESIVA_RECARGO_CLIENTE_FAC` AS `ESIVA_RECARGO_CLIENTE_FAC`,`fza_facturas`.`ESIVA_EXENTO_CLIENTE_FAC` AS `ESIVA_EXENTO_CLIENTE_FAC`,`fza_facturas`.`ESREGIMENESPECIALAGRICOLA_CLIENTE_FAC` AS `ESREGIMENESPECIALAGRICOLA_CLIENTE_FAC`,`fza_facturas`.`ESRETENCIONES_CLIENTE_FAC` AS `ESRETENCIONES_CLIENTE_FAC`,`fza_facturas`.`TARIFA_ARTICULO_CLIENTE_FAC` AS `TARIFA_ARTICULO_CLIENTE_FAC`,`fza_facturas`.`ESIMP_INCL_TARIFA_CLIENTE_FAC` AS `ESIMP_INCL_TARIFA_CLIENTE_FAC`,`fza_facturas`.`ESINTRACOMUNITARIO_CLIENTE_FAC` AS `ESINTRACOMUNITARIO_CLIENTE_FAC`,`fza_facturas`.`ESIRPF_IMP_INCL_ZONA_IVA_FAC` AS `ESIRPF_IMP_INCL_ZONA_IVA_FAC`,`fza_facturas`.`ESAPLICA_RE_ZONA_IVA_FAC` AS `ESAPLICA_RE_ZONA_IVA_FAC`,`fza_facturas`.`ESIVAAGRICOLA_ZONA_IVA_FAC` AS `ESIVAAGRICOLA_ZONA_IVA_FAC`,`fza_facturas`.`PALABRA_REPORTS_ZONA_IVA_FAC` AS `PALABRA_REPORTS_ZONA_IVA_FAC`,`fza_facturas`.`CODIGO_IVA_FAC` AS `CODIGO_IVA_FAC`,`fza_facturas`.`ESVENTA_ACTIVO_FIJO_FAC` AS `ESVENTA_ACTIVO_FIJO_FAC`,`fza_facturas`.`PORCENTAJE_IVAN_FAC` AS `PORCENTAJE_IVAN_FAC`,`fza_facturas`.`TOTAL_IVAN_FAC` AS `TOTAL_IVAN_FAC`,`fza_facturas`.`PORCENTAJE_REN_FAC` AS `PORCENTAJE_REN_FAC`,`fza_facturas`.`TOTAL_REN_FAC` AS `TOTAL_REN_FAC`,`fza_facturas`.`TOTAL_BASEI_IVAN_FAC` AS `TOTAL_BASEI_IVAN_FAC`,`fza_facturas`.`PORCENTAJE_IVAR_FAC` AS `PORCENTAJE_IVAR_FAC`,`fza_facturas`.`TOTAL_IVAR_FAC` AS `TOTAL_IVAR_FAC`,`fza_facturas`.`PORCENTAJE_RER_FAC` AS `PORCENTAJE_RER_FAC`,`fza_facturas`.`TOTAL_RER_FAC` AS `TOTAL_RER_FAC`,`fza_facturas`.`TOTAL_BASEI_IVAR_FAC` AS `TOTAL_BASEI_IVAR_FAC`,`fza_facturas`.`PORCENTAJE_IVAS_FAC` AS `PORCENTAJE_IVAS_FAC`,`fza_facturas`.`TOTAL_IVAS_FAC` AS `TOTAL_IVAS_FAC`,`fza_facturas`.`PORCENTAJE_RES_FAC` AS `PORCENTAJE_RES_FAC`,`fza_facturas`.`TOTAL_RES_FAC` AS `TOTAL_RES_FAC`,`fza_facturas`.`TOTAL_BASEI_IVAS_FAC` AS `TOTAL_BASEI_IVAS_FAC`,`fza_facturas`.`PORCENTAJE_IVAE_FAC` AS `PORCENTAJE_IVAE_FAC`,`fza_facturas`.`TOTAL_IVAE_FAC` AS `TOTAL_IVAE_FAC`,`fza_facturas`.`PORCENTAJE_REE_FAC` AS `PORCENTAJE_REE_FAC`,`fza_facturas`.`TOTAL_REE_FAC` AS `TOTAL_REE_FAC`,`fza_facturas`.`TOTAL_BASEI_IVAE_FAC` AS `TOTAL_BASEI_IVAE_FAC`,`fza_facturas`.`NUMERO_FAC_ABONO_FAC` AS `NUMERO_FAC_ABONO_FAC`,`fza_facturas`.`SERIE_FAC_ABONO_FAC` AS `SERIE_FAC_ABONO_FAC`,`fza_facturas`.`TEXTO_LEGAL_CLIENTE_FAC` AS `TEXTO_LEGAL_CLIENTE_FAC`,`fza_facturas`.`TEXTO_LEGAL_EMPRESA_FAC` AS `TEXTO_LEGAL_EMPRESA_FAC`,`fza_facturas`.`DOCUMENTO_FAC` AS `DOCUMENTO_FAC`,`fza_facturas`.`XML_FAC` AS `XML_FAC`,`fza_facturas`.`COMENTARIOS_FAC` AS `COMENTARIOS_FAC`,`fza_facturas`.`CONTADOR_LINEAS_FAC` AS `CONTADOR_LINEAS_FAC`,`fza_facturas`.`ESCREARARTICULOS_FAC` AS `ESCREARARTICULOS_FAC`,`fza_facturas`.`ESDESCRIPCIONES_AMP_FAC` AS `ESDESCRIPCIONES_AMP_FAC`,`fza_facturas`.`ESFECHADEENTREGA_FAC` AS `ESFECHADEENTREGA_FAC`,`fza_facturas`.`CODIGO_ALM_FAC` AS `CODIGO_ALM_FAC`,`fza_facturas`.`CODIGO_CAJA_FAC` AS `CODIGO_CAJA_FAC`,`fza_facturas`.`NUMERO_OPERACION_FAC` AS `NUMERO_OPERACION_FAC`,`fza_facturas`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_facturas`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_facturas`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_facturas`.`USUARIO_MODIF` AS `USUARIO_MODIF`,`fza_formas_pago`.`DESCRIPCION_FORMA_PAGO_FP` AS `DESCRIPCION_FORMA_PAGO_FP`,`fza_facturas`.`ESCONSOLIDADA_FAC` AS `ESCONSOLIDADA_FAC`,`fza_facturas`.`INSTANTECONSO_FAC` AS `INSTANTECONSO_FAC` from (`fza_facturas` left join `fza_formas_pago` on(`fza_facturas`.`FORMA_PAGO_FAC` = `fza_formas_pago`.`CODIGO_FP_FP`)) order by `fza_facturas`.`FECHA_FAC` desc;

-- Recreando vista: vi_facturas_lineas
DROP VIEW IF EXISTS `vi_facturas_lineas`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_facturas_lineas` AS select `fza_facturas_lineas`.`NUMERO_FAC_FACLIN` AS `NUMERO_FAC_FACLIN`,`fza_facturas_lineas`.`SERIE_FAC_FACLIN` AS `SERIE_FAC_FACLIN`,`fza_facturas_lineas`.`LINEA_FACLIN` AS `LINEA_FACLIN`,`fza_facturas_lineas`.`CODIGO_ART_FACLIN` AS `CODIGO_ART_FACLIN`,`fza_facturas_lineas`.`CODIGO_UNIDAD_FACLIN` AS `CODIGO_UNIDAD_FACLIN`,`fza_facturas_lineas`.`DESCRIPCION_VARIACION_FACLIN` AS `DESCRIPCION_VARIACION_FACLIN`,`fza_facturas_lineas`.`CODIGO_FAM_FACLIN` AS `CODIGO_FAM_FACLIN`,`fza_facturas_lineas`.`NOMBRE_FAM_FACLIN` AS `NOMBRE_FAM_FACLIN`,`fza_facturas_lineas`.`ESPROVEEDORPRINCIPAL_FACLIN` AS `ESPROVEEDORPRINCIPAL_FACLIN`,`fza_facturas_lineas`.`CODIGO_PRV_FACLIN` AS `CODIGO_PRV_FACLIN`,`fza_facturas_lineas`.`RAZON_SOCIAL_PROVEEDOR_FACLIN` AS `RAZON_SOCIAL_PROVEEDOR_FACLIN`,`fza_facturas_lineas`.`PRECIO_ULT_COMPRA_FACLIN` AS `PRECIO_ULT_COMPRA_FACLIN`,`fza_facturas_lineas`.`FECHA_ENTREGA_FACLIN` AS `FECHA_ENTREGA_FACLIN`,`fza_facturas_lineas`.`TIPO_CANTIDAD_ARTICULO_FACLIN` AS `TIPO_CANTIDAD_ARTICULO_FACLIN`,`fza_facturas_lineas`.`ESIMP_INCL_TARIFA_FACLIN` AS `ESIMP_INCL_TARIFA_FACLIN`,`fza_facturas_lineas`.`TIPO_IVA_ARTICULO_FACLIN` AS `TIPO_IVA_ARTICULO_FACLIN`,`fza_facturas_lineas`.`DESCRIPCION_ARTICULO_FACLIN` AS `DESCRIPCION_ARTICULO_FACLIN`,`fza_facturas_lineas`.`CODIGO_TAR_FACLIN` AS `CODIGO_TAR_FACLIN`,`fza_facturas_lineas`.`CANTIDAD_FACLIN` AS `CANTIDAD_FACLIN`,`fza_facturas_lineas`.`PRECIO_SALIDA_FACLIN` AS `PRECIO_SALIDA_FACLIN`,`fza_facturas_lineas`.`PORCENTAJE_DTO_FACLIN` AS `PORCENTAJE_DTO_FACLIN`,`fza_facturas_lineas`.`PRECIO_DTO_FACLIN` AS `PRECIO_DTO_FACLIN`,`fza_facturas_lineas`.`PRECIO_VENTA_SIVA_ARTICULO_FACLIN` AS `PRECIO_VENTA_SIVA_ARTICULO_FACLIN`,`fza_facturas_lineas`.`PORCENTAJE_IVA_FACLIN` AS `PORCENTAJE_IVA_FACLIN`,`fza_facturas_lineas`.`PRECIO_VENTA_CIVA_ARTICULO_FACLIN` AS `PRECIO_VENTA_CIVA_ARTICULO_FACLIN`,`fza_facturas_lineas`.`TOTAL_FACLIN` AS `TOTAL_FACLIN`,`fza_facturas_lineas`.`TOTAL_FAC_SIVA_FACLIN` AS `TOTAL_FAC_SIVA_FACLIN`,`fza_facturas_lineas`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_facturas_lineas`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_facturas_lineas`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_facturas_lineas`.`USUARIO_MODIF` AS `USUARIO_MODIF` from `fza_facturas_lineas`;

-- Recreando vista: vi_facturas_lineas_print
DROP VIEW IF EXISTS `vi_facturas_lineas_print`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_facturas_lineas_print` AS select `fza_facturas_lineas`.`NUMERO_FAC_FACLIN` AS `NUMERO_FAC_FACLIN`,`fza_facturas_lineas`.`SERIE_FAC_FACLIN` AS `SERIE_FAC_FACLIN`,`fza_facturas_lineas`.`LINEA_FACLIN` AS `LINEA_FACLIN`,`fza_facturas_lineas`.`CODIGO_ART_FACLIN` AS `CODIGO_ART_FACLIN`,`fza_facturas_lineas`.`CODIGO_FAM_FACLIN` AS `CODIGO_FAM_FACLIN`,`fza_facturas_lineas`.`NOMBRE_FAM_FACLIN` AS `NOMBRE_FAM_FACLIN`,`fza_facturas_lineas`.`FECHA_ENTREGA_FACLIN` AS `FECHA_ENTREGA_FACLIN`,`fza_facturas_lineas`.`TIPO_CANTIDAD_ARTICULO_FACLIN` AS `TIPO_CANTIDAD_ARTICULO_FACLIN`,`fza_facturas_lineas`.`ESIMP_INCL_TARIFA_FACLIN` AS `ESIMP_INCL_TARIFA_FACLIN`,`fza_facturas_lineas`.`TIPO_IVA_ARTICULO_FACLIN` AS `TIPO_IVA_ARTICULO_FACLIN`,`fza_facturas_lineas`.`DESCRIPCION_ARTICULO_FACLIN` AS `DESCRIPCION_ARTICULO_FACLIN`,`fza_facturas_lineas`.`CODIGO_TAR_FACLIN` AS `CODIGO_TAR_FACLIN`,`fza_facturas_lineas`.`CANTIDAD_FACLIN` AS `CANTIDAD_FACLIN`,`fza_facturas_lineas`.`PRECIO_SALIDA_FACLIN` AS `PRECIO_SALIDA_FACLIN`,`fza_facturas_lineas`.`PORCENTAJE_DTO_FACLIN` AS `PORCENTAJE_DTO_FACLIN`,`fza_facturas_lineas`.`PRECIO_DTO_FACLIN` AS `PRECIO_DTO_FACLIN`,`fza_facturas_lineas`.`PRECIO_VENTA_SIVA_ARTICULO_FACLIN` AS `PRECIO_VENTA_SIVA_ARTICULO_FACLIN`,`fza_facturas_lineas`.`PORCENTAJE_IVA_FACLIN` AS `PORCENTAJE_IVA_FACLIN`,`fza_facturas_lineas`.`PRECIO_VENTA_CIVA_ARTICULO_FACLIN` AS `PRECIO_VENTA_CIVA_ARTICULO_FACLIN`,`fza_facturas_lineas`.`TOTAL_FACLIN` AS `TOTAL_FACLIN`,`fza_facturas_lineas`.`TOTAL_FAC_SIVA_FACLIN` AS `TOTAL_FAC_SIVA_FACLIN`,`fza_facturas_lineas`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_facturas_lineas`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_facturas_lineas`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_facturas_lineas`.`USUARIO_MODIF` AS `USUARIO_MODIF` from `fza_facturas_lineas`;

-- Recreando vista: vi_facturas_normales
DROP VIEW IF EXISTS `vi_facturas_normales`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_facturas_normales` AS select `vi_facturas`.`FECHA_FAC` AS `FECHA_FAC`,`vi_facturas`.`NUMERO_FAC` AS `NUMERO_FAC`,`vi_facturas`.`SERIE_FAC` AS `SERIE_FAC`,`vi_facturas`.`TIPO_FAC` AS `TIPO_FAC`,`vi_facturas`.`FASE_FAC` AS `FASE_FAC`,`vi_facturas`.`TOTAL_LIQUIDO_FAC` AS `TOTAL_LIQUIDO_FAC`,`vi_facturas`.`PORCENTAJE_RETENCION_FAC` AS `PORCENTAJE_RETENCION_FAC`,`vi_facturas`.`TOTAL_RETENCION_FAC` AS `TOTAL_RETENCION_FAC`,`vi_facturas`.`TOTAL_IMPUESTOS_FAC` AS `TOTAL_IMPUESTOS_FAC`,`vi_facturas`.`TOTAL_BASES_FAC` AS `TOTAL_BASES_FAC`,`vi_facturas`.`CODIGO_CAJERO_FAC` AS `CODIGO_CAJERO_FAC`,`vi_facturas`.`FORMA_PAGO_FAC` AS `FORMA_PAGO_FAC`,`vi_facturas`.`CODIGO_EMP_FAC` AS `CODIGO_EMP_FAC`,`vi_facturas`.`RAZON_SOCIAL_EMPRESA_FAC` AS `RAZON_SOCIAL_EMPRESA_FAC`,`vi_facturas`.`NIF_EMPRESA_FAC` AS `NIF_EMPRESA_FAC`,`vi_facturas`.`MOVIL_EMPRESA_FAC` AS `MOVIL_EMPRESA_FAC`,`vi_facturas`.`EMAIL_EMPRESA_FAC` AS `EMAIL_EMPRESA_FAC`,`vi_facturas`.`DIRECCION1_EMPRESA_FAC` AS `DIRECCION1_EMPRESA_FAC`,`vi_facturas`.`DIRECCION2_EMPRESA_FAC` AS `DIRECCION2_EMPRESA_FAC`,`vi_facturas`.`POBLACION_EMPRESA_FAC` AS `POBLACION_EMPRESA_FAC`,`vi_facturas`.`PROVINCIA_EMPRESA_FAC` AS `PROVINCIA_EMPRESA_FAC`,`vi_facturas`.`NOMBRE_PAI_EMPRESA_FAC` AS `NOMBRE_PAI_EMPRESA_FAC`,`vi_facturas`.`CODIGO_PAI_EMPRESA_FAC` AS `CODIGO_PAI_EMPRESA_FAC`,`vi_facturas`.`CODIGO_POSTAL_EMPRESA_FAC` AS `CODIGO_POSTAL_EMPRESA_FAC`,`vi_facturas`.`ESRETENCIONES_EMPRESA_FAC` AS `ESRETENCIONES_EMPRESA_FAC`,`vi_facturas`.`GRUPO_ZONA_IVA_EMPRESA_FAC` AS `GRUPO_ZONA_IVA_EMPRESA_FAC`,`vi_facturas`.`ESREGIMENESPECIALAGRICOLA_EMPRESA_FAC` AS `ESREGIMENESPECIALAGRICOLA_EMPRESA_FAC`,`vi_facturas`.`CODIGO_CLI_FAC` AS `CODIGO_CLI_FAC`,`vi_facturas`.`RAZON_SOCIAL_CLIENTE_FAC` AS `RAZON_SOCIAL_CLIENTE_FAC`,`vi_facturas`.`NIF_CLIENTE_FAC` AS `NIF_CLIENTE_FAC`,`vi_facturas`.`MOVIL_CLIENTE_FAC` AS `MOVIL_CLIENTE_FAC`,`vi_facturas`.`EMAIL_CLIENTE_FAC` AS `EMAIL_CLIENTE_FAC`,`vi_facturas`.`DIRECCION1_CLIENTE_FAC` AS `DIRECCION1_CLIENTE_FAC`,`vi_facturas`.`DIRECCION2_CLIENTE_FAC` AS `DIRECCION2_CLIENTE_FAC`,`vi_facturas`.`POBLACION_CLIENTE_FAC` AS `POBLACION_CLIENTE_FAC`,`vi_facturas`.`PROVINCIA_CLIENTE_FAC` AS `PROVINCIA_CLIENTE_FAC`,`vi_facturas`.`CODIGO_POSTAL_CLIENTE_FAC` AS `CODIGO_POSTAL_CLIENTE_FAC`,`vi_facturas`.`NOMBRE_PAI_CLIENTE_FAC` AS `NOMBRE_PAI_CLIENTE_FAC`,`vi_facturas`.`CODIGO_PAI_CLIENTE_FAC` AS `CODIGO_PAI_CLIENTE_FAC`,`vi_facturas`.`ESIVA_RECARGO_CLIENTE_FAC` AS `ESIVA_RECARGO_CLIENTE_FAC`,`vi_facturas`.`ESIVA_EXENTO_CLIENTE_FAC` AS `ESIVA_EXENTO_CLIENTE_FAC`,`vi_facturas`.`ESREGIMENESPECIALAGRICOLA_CLIENTE_FAC` AS `ESREGIMENESPECIALAGRICOLA_CLIENTE_FAC`,`vi_facturas`.`ESRETENCIONES_CLIENTE_FAC` AS `ESRETENCIONES_CLIENTE_FAC`,`vi_facturas`.`TARIFA_ARTICULO_CLIENTE_FAC` AS `TARIFA_ARTICULO_CLIENTE_FAC`,`vi_facturas`.`ESIMP_INCL_TARIFA_CLIENTE_FAC` AS `ESIMP_INCL_TARIFA_CLIENTE_FAC`,`vi_facturas`.`ESINTRACOMUNITARIO_CLIENTE_FAC` AS `ESINTRACOMUNITARIO_CLIENTE_FAC`,`vi_facturas`.`ESIRPF_IMP_INCL_ZONA_IVA_FAC` AS `ESIRPF_IMP_INCL_ZONA_IVA_FAC`,`vi_facturas`.`ESAPLICA_RE_ZONA_IVA_FAC` AS `ESAPLICA_RE_ZONA_IVA_FAC`,`vi_facturas`.`ESIVAAGRICOLA_ZONA_IVA_FAC` AS `ESIVAAGRICOLA_ZONA_IVA_FAC`,`vi_facturas`.`PALABRA_REPORTS_ZONA_IVA_FAC` AS `PALABRA_REPORTS_ZONA_IVA_FAC`,`vi_facturas`.`CODIGO_IVA_FAC` AS `CODIGO_IVA_FAC`,`vi_facturas`.`ESVENTA_ACTIVO_FIJO_FAC` AS `ESVENTA_ACTIVO_FIJO_FAC`,`vi_facturas`.`PORCENTAJE_IVAN_FAC` AS `PORCENTAJE_IVAN_FAC`,`vi_facturas`.`TOTAL_IVAN_FAC` AS `TOTAL_IVAN_FAC`,`vi_facturas`.`PORCENTAJE_REN_FAC` AS `PORCENTAJE_REN_FAC`,`vi_facturas`.`TOTAL_REN_FAC` AS `TOTAL_REN_FAC`,`vi_facturas`.`TOTAL_BASEI_IVAN_FAC` AS `TOTAL_BASEI_IVAN_FAC`,`vi_facturas`.`PORCENTAJE_IVAR_FAC` AS `PORCENTAJE_IVAR_FAC`,`vi_facturas`.`TOTAL_IVAR_FAC` AS `TOTAL_IVAR_FAC`,`vi_facturas`.`PORCENTAJE_RER_FAC` AS `PORCENTAJE_RER_FAC`,`vi_facturas`.`TOTAL_RER_FAC` AS `TOTAL_RER_FAC`,`vi_facturas`.`TOTAL_BASEI_IVAR_FAC` AS `TOTAL_BASEI_IVAR_FAC`,`vi_facturas`.`PORCENTAJE_IVAS_FAC` AS `PORCENTAJE_IVAS_FAC`,`vi_facturas`.`TOTAL_IVAS_FAC` AS `TOTAL_IVAS_FAC`,`vi_facturas`.`PORCENTAJE_RES_FAC` AS `PORCENTAJE_RES_FAC`,`vi_facturas`.`TOTAL_RES_FAC` AS `TOTAL_RES_FAC`,`vi_facturas`.`TOTAL_BASEI_IVAS_FAC` AS `TOTAL_BASEI_IVAS_FAC`,`vi_facturas`.`PORCENTAJE_IVAE_FAC` AS `PORCENTAJE_IVAE_FAC`,`vi_facturas`.`TOTAL_IVAE_FAC` AS `TOTAL_IVAE_FAC`,`vi_facturas`.`PORCENTAJE_REE_FAC` AS `PORCENTAJE_REE_FAC`,`vi_facturas`.`TOTAL_REE_FAC` AS `TOTAL_REE_FAC`,`vi_facturas`.`TOTAL_BASEI_IVAE_FAC` AS `TOTAL_BASEI_IVAE_FAC`,`vi_facturas`.`NUMERO_FAC_ABONO_FAC` AS `NUMERO_FAC_ABONO_FAC`,`vi_facturas`.`SERIE_FAC_ABONO_FAC` AS `SERIE_FAC_ABONO_FAC`,`vi_facturas`.`TEXTO_LEGAL_CLIENTE_FAC` AS `TEXTO_LEGAL_CLIENTE_FAC`,`vi_facturas`.`TEXTO_LEGAL_EMPRESA_FAC` AS `TEXTO_LEGAL_EMPRESA_FAC`,`vi_facturas`.`DOCUMENTO_FAC` AS `DOCUMENTO_FAC`,`vi_facturas`.`XML_FAC` AS `XML_FAC`,`vi_facturas`.`COMENTARIOS_FAC` AS `COMENTARIOS_FAC`,`vi_facturas`.`CONTADOR_LINEAS_FAC` AS `CONTADOR_LINEAS_FAC`,`vi_facturas`.`ESCREARARTICULOS_FAC` AS `ESCREARARTICULOS_FAC`,`vi_facturas`.`ESDESCRIPCIONES_AMP_FAC` AS `ESDESCRIPCIONES_AMP_FAC`,`vi_facturas`.`ESFECHADEENTREGA_FAC` AS `ESFECHADEENTREGA_FAC`,`vi_facturas`.`CODIGO_ALM_FAC` AS `CODIGO_ALM_FAC`,`vi_facturas`.`CODIGO_CAJA_FAC` AS `CODIGO_CAJA_FAC`,`vi_facturas`.`NUMERO_OPERACION_FAC` AS `NUMERO_OPERACION_FAC`,`vi_facturas`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`vi_facturas`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`vi_facturas`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`vi_facturas`.`USUARIO_MODIF` AS `USUARIO_MODIF`,`vi_facturas`.`DESCRIPCION_FORMA_PAGO_FP` AS `DESCRIPCION_FORMA_PAGO_FP`,`vi_facturas`.`ESCONSOLIDADA_FAC` AS `ESCONSOLIDADA_FAC`,`vi_facturas`.`INSTANTECONSO_FAC` AS `INSTANTECONSO_FAC` from `vi_facturas` where `vi_facturas`.`TIPO_FAC` = 'NORMAL';

-- Recreando vista: vi_facturas_print
DROP VIEW IF EXISTS `vi_facturas_print`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_facturas_print` AS select `fza_facturas`.`FECHA_FAC` AS `FECHA_FAC`,`fza_facturas`.`NUMERO_FAC` AS `NUMERO_FAC`,`fza_facturas`.`SERIE_FAC` AS `SERIE_FAC`,`fza_facturas`.`TOTAL_LIQUIDO_FAC` AS `TOTAL_LIQUIDO_FAC`,`fza_facturas`.`PORCENTAJE_RETENCION_FAC` AS `PORCENTAJE_RETENCION_FAC`,`fza_facturas`.`TOTAL_RETENCION_FAC` AS `TOTAL_RETENCION_FAC`,`fza_facturas`.`TOTAL_IMPUESTOS_FAC` AS `TOTAL_IMPUESTOS_FAC`,`fza_facturas`.`TOTAL_BASES_FAC` AS `TOTAL_BASES_FAC`,`fza_facturas`.`FORMA_PAGO_FAC` AS `FORMA_PAGO_FAC`,`fza_facturas`.`CODIGO_EMP_FAC` AS `CODIGO_EMP_FAC`,`fza_facturas`.`RAZON_SOCIAL_EMPRESA_FAC` AS `RAZON_SOCIAL_EMPRESA_FAC`,`fza_facturas`.`NIF_EMPRESA_FAC` AS `NIF_EMPRESA_FAC`,`fza_facturas`.`MOVIL_EMPRESA_FAC` AS `MOVIL_EMPRESA_FAC`,`fza_facturas`.`EMAIL_EMPRESA_FAC` AS `EMAIL_EMPRESA_FAC`,`fza_facturas`.`DIRECCION1_EMPRESA_FAC` AS `DIRECCION1_EMPRESA_FAC`,`fza_facturas`.`DIRECCION2_EMPRESA_FAC` AS `DIRECCION2_EMPRESA_FAC`,`fza_facturas`.`POBLACION_EMPRESA_FAC` AS `POBLACION_EMPRESA_FAC`,`fza_facturas`.`PROVINCIA_EMPRESA_FAC` AS `PROVINCIA_EMPRESA_FAC`,`fza_facturas`.`NOMBRE_PAI_EMPRESA_FAC` AS `NOMBRE_PAI_EMPRESA_FAC`,`fza_facturas`.`CODIGO_PAI_EMPRESA_FAC` AS `CODIGO_PAI_EMPRESA_FAC`,`fza_facturas`.`CODIGO_POSTAL_EMPRESA_FAC` AS `CODIGO_POSTAL_EMPRESA_FAC`,`fza_facturas`.`ESRETENCIONES_EMPRESA_FAC` AS `ESRETENCIONES_EMPRESA_FAC`,`fza_facturas`.`GRUPO_ZONA_IVA_EMPRESA_FAC` AS `GRUPO_ZONA_IVA_EMPRESA_FAC`,`fza_facturas`.`ESREGIMENESPECIALAGRICOLA_EMPRESA_FAC` AS `ESREGIMENESPECIALAGRICOLA_EMPRESA_FAC`,`fza_facturas`.`CODIGO_CLI_FAC` AS `CODIGO_CLI_FAC`,`fza_facturas`.`RAZON_SOCIAL_CLIENTE_FAC` AS `RAZON_SOCIAL_CLIENTE_FAC`,`fza_facturas`.`NIF_CLIENTE_FAC` AS `NIF_CLIENTE_FAC`,`fza_facturas`.`MOVIL_CLIENTE_FAC` AS `MOVIL_CLIENTE_FAC`,`fza_facturas`.`EMAIL_CLIENTE_FAC` AS `EMAIL_CLIENTE_FAC`,`fza_facturas`.`DIRECCION1_CLIENTE_FAC` AS `DIRECCION1_CLIENTE_FAC`,`fza_facturas`.`DIRECCION2_CLIENTE_FAC` AS `DIRECCION2_CLIENTE_FAC`,`fza_facturas`.`POBLACION_CLIENTE_FAC` AS `POBLACION_CLIENTE_FAC`,`fza_facturas`.`PROVINCIA_CLIENTE_FAC` AS `PROVINCIA_CLIENTE_FAC`,`fza_facturas`.`CODIGO_POSTAL_CLIENTE_FAC` AS `CODIGO_POSTAL_CLIENTE_FAC`,`fza_facturas`.`NOMBRE_PAI_CLIENTE_FAC` AS `NOMBRE_PAI_CLIENTE_FAC`,`fza_facturas`.`CODIGO_PAI_CLIENTE_FAC` AS `CODIGO_PAI_CLIENTE_FAC`,`fza_facturas`.`ESIVA_RECARGO_CLIENTE_FAC` AS `ESIVA_RECARGO_CLIENTE_FAC`,`fza_facturas`.`ESIVA_EXENTO_CLIENTE_FAC` AS `ESIVA_EXENTO_CLIENTE_FAC`,`fza_facturas`.`ESREGIMENESPECIALAGRICOLA_CLIENTE_FAC` AS `ESREGIMENESPECIALAGRICOLA_CLIENTE_FAC`,`fza_facturas`.`ESRETENCIONES_CLIENTE_FAC` AS `ESRETENCIONES_CLIENTE_FAC`,`fza_facturas`.`TARIFA_ARTICULO_CLIENTE_FAC` AS `TARIFA_ARTICULO_CLIENTE_FAC`,`fza_facturas`.`ESIMP_INCL_TARIFA_CLIENTE_FAC` AS `ESIMP_INCL_TARIFA_CLIENTE_FAC`,`fza_facturas`.`ESINTRACOMUNITARIO_CLIENTE_FAC` AS `ESINTRACOMUNITARIO_CLIENTE_FAC`,`fza_facturas`.`ESIRPF_IMP_INCL_ZONA_IVA_FAC` AS `ESIRPF_IMP_INCL_ZONA_IVA_FAC`,`fza_facturas`.`ESAPLICA_RE_ZONA_IVA_FAC` AS `ESAPLICA_RE_ZONA_IVA_FAC`,`fza_facturas`.`ESIVAAGRICOLA_ZONA_IVA_FAC` AS `ESIVAAGRICOLA_ZONA_IVA_FAC`,`fza_facturas`.`PALABRA_REPORTS_ZONA_IVA_FAC` AS `PALABRA_REPORTS_ZONA_IVA_FAC`,`fza_facturas`.`CODIGO_IVA_FAC` AS `CODIGO_IVA_FAC`,`fza_facturas`.`ESVENTA_ACTIVO_FIJO_FAC` AS `ESVENTA_ACTIVO_FIJO_FAC`,`fza_facturas`.`PORCENTAJE_IVAN_FAC` AS `PORCENTAJE_IVAN_FAC`,`fza_facturas`.`TOTAL_IVAN_FAC` AS `TOTAL_IVAN_FAC`,`fza_facturas`.`PORCENTAJE_REN_FAC` AS `PORCENTAJE_REN_FAC`,`fza_facturas`.`TOTAL_REN_FAC` AS `TOTAL_REN_FAC`,`fza_facturas`.`TOTAL_BASEI_IVAN_FAC` AS `TOTAL_BASEI_IVAN_FAC`,`fza_facturas`.`PORCENTAJE_IVAR_FAC` AS `PORCENTAJE_IVAR_FAC`,`fza_facturas`.`TOTAL_IVAR_FAC` AS `TOTAL_IVAR_FAC`,`fza_facturas`.`PORCENTAJE_RER_FAC` AS `PORCENTAJE_RER_FAC`,`fza_facturas`.`TOTAL_RER_FAC` AS `TOTAL_RER_FAC`,`fza_facturas`.`TOTAL_BASEI_IVAR_FAC` AS `TOTAL_BASEI_IVAR_FAC`,`fza_facturas`.`PORCENTAJE_IVAS_FAC` AS `PORCENTAJE_IVAS_FAC`,`fza_facturas`.`TOTAL_IVAS_FAC` AS `TOTAL_IVAS_FAC`,`fza_facturas`.`PORCENTAJE_RES_FAC` AS `PORCENTAJE_RES_FAC`,`fza_facturas`.`TOTAL_RES_FAC` AS `TOTAL_RES_FAC`,`fza_facturas`.`TOTAL_BASEI_IVAS_FAC` AS `TOTAL_BASEI_IVAS_FAC`,`fza_facturas`.`PORCENTAJE_IVAE_FAC` AS `PORCENTAJE_IVAE_FAC`,`fza_facturas`.`TOTAL_IVAE_FAC` AS `TOTAL_IVAE_FAC`,`fza_facturas`.`PORCENTAJE_REE_FAC` AS `PORCENTAJE_REE_FAC`,`fza_facturas`.`TOTAL_REE_FAC` AS `TOTAL_REE_FAC`,`fza_facturas`.`TOTAL_BASEI_IVAE_FAC` AS `TOTAL_BASEI_IVAE_FAC`,`fza_facturas`.`NUMERO_FAC_ABONO_FAC` AS `NUMERO_FAC_ABONO_FAC`,`fza_facturas`.`SERIE_FAC_ABONO_FAC` AS `SERIE_FAC_ABONO_FAC`,`fza_facturas`.`TEXTO_LEGAL_CLIENTE_FAC` AS `TEXTO_LEGAL_CLIENTE_FAC`,`fza_facturas`.`TEXTO_LEGAL_EMPRESA_FAC` AS `TEXTO_LEGAL_EMPRESA_FAC`,`fza_facturas`.`DOCUMENTO_FAC` AS `DOCUMENTO_FAC`,`fza_facturas`.`COMENTARIOS_FAC` AS `COMENTARIOS_FAC`,`fza_facturas`.`CONTADOR_LINEAS_FAC` AS `CONTADOR_LINEAS_FAC`,`fza_facturas`.`ESCREARARTICULOS_FAC` AS `ESCREARARTICULOS_FAC`,`fza_facturas`.`ESDESCRIPCIONES_AMP_FAC` AS `ESDESCRIPCIONES_AMP_FAC`,`fza_facturas`.`ESFECHADEENTREGA_FAC` AS `ESFECHADEENTREGA_FAC`,`fza_formas_pago`.`DESCRIPCION_FORMA_PAGO_FP` AS `DESCRIPCION_FORMA_PAGO_FP`,`fza_formas_pago`.`ESCONTADO_FORMA_PAGO_FP` AS `ESCONTADO_FORMA_PAGO_FP`,(select group_concat(' ',date_format(`fza_recibos`.`FECHA_VENCIMIENTO_RECIBO_REC`,'%d/%m/%Y'),'=> ',format(`fza_recibos`.`EUROS_RECIBO_REC`,2),'€' separator ',') from `fza_recibos` where `fza_recibos`.`NUMERO_FAC_REC` = `fza_facturas`.`NUMERO_FAC` and `fza_recibos`.`SERIE_FAC_REC` = `fza_facturas`.`SERIE_FAC`) AS `VENCIMIENTOS_RECIBOS`,`fza_empresas`.`IBAN_EMP` AS `IBAN_EMP`,`fza_clientes`.`IBAN_CLI` AS `IBAN_CLI`,`fza_formas_pago`.`ESVERBANCOEMPRESA_FORMA_PAGO_FP` AS `ESVERBANCOEMPRESA_FORMA_PAGO_FP` from (((`fza_facturas` left join `fza_formas_pago` on(`fza_facturas`.`FORMA_PAGO_FAC` = `fza_formas_pago`.`CODIGO_FP_FP`)) left join `fza_empresas` on(`fza_facturas`.`CODIGO_EMP_FAC` = `fza_empresas`.`CODIGO_EMP_EMP`)) left join `fza_clientes` on(`fza_facturas`.`CODIGO_CLI_FAC` = `fza_clientes`.`CODIGO_CLI_CLI`)) order by `fza_facturas`.`FECHA_FAC` desc;

-- Recreando vista: vi_facturas_simplificadas
DROP VIEW IF EXISTS `vi_facturas_simplificadas`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_facturas_simplificadas` AS select `vi_facturas`.`FECHA_FAC` AS `FECHA_FAC`,`vi_facturas`.`NUMERO_FAC` AS `NUMERO_FAC`,`vi_facturas`.`SERIE_FAC` AS `SERIE_FAC`,`vi_facturas`.`TIPO_FAC` AS `TIPO_FAC`,`vi_facturas`.`FASE_FAC` AS `FASE_FAC`,`vi_facturas`.`TOTAL_LIQUIDO_FAC` AS `TOTAL_LIQUIDO_FAC`,`vi_facturas`.`PORCENTAJE_RETENCION_FAC` AS `PORCENTAJE_RETENCION_FAC`,`vi_facturas`.`TOTAL_RETENCION_FAC` AS `TOTAL_RETENCION_FAC`,`vi_facturas`.`TOTAL_IMPUESTOS_FAC` AS `TOTAL_IMPUESTOS_FAC`,`vi_facturas`.`TOTAL_BASES_FAC` AS `TOTAL_BASES_FAC`,`vi_facturas`.`CODIGO_CAJERO_FAC` AS `CODIGO_CAJERO_FAC`,`vi_facturas`.`FORMA_PAGO_FAC` AS `FORMA_PAGO_FAC`,`vi_facturas`.`CODIGO_EMP_FAC` AS `CODIGO_EMP_FAC`,`vi_facturas`.`RAZON_SOCIAL_EMPRESA_FAC` AS `RAZON_SOCIAL_EMPRESA_FAC`,`vi_facturas`.`NIF_EMPRESA_FAC` AS `NIF_EMPRESA_FAC`,`vi_facturas`.`MOVIL_EMPRESA_FAC` AS `MOVIL_EMPRESA_FAC`,`vi_facturas`.`EMAIL_EMPRESA_FAC` AS `EMAIL_EMPRESA_FAC`,`vi_facturas`.`DIRECCION1_EMPRESA_FAC` AS `DIRECCION1_EMPRESA_FAC`,`vi_facturas`.`DIRECCION2_EMPRESA_FAC` AS `DIRECCION2_EMPRESA_FAC`,`vi_facturas`.`POBLACION_EMPRESA_FAC` AS `POBLACION_EMPRESA_FAC`,`vi_facturas`.`PROVINCIA_EMPRESA_FAC` AS `PROVINCIA_EMPRESA_FAC`,`vi_facturas`.`NOMBRE_PAI_EMPRESA_FAC` AS `NOMBRE_PAI_EMPRESA_FAC`,`vi_facturas`.`CODIGO_PAI_EMPRESA_FAC` AS `CODIGO_PAI_EMPRESA_FAC`,`vi_facturas`.`CODIGO_POSTAL_EMPRESA_FAC` AS `CODIGO_POSTAL_EMPRESA_FAC`,`vi_facturas`.`ESRETENCIONES_EMPRESA_FAC` AS `ESRETENCIONES_EMPRESA_FAC`,`vi_facturas`.`GRUPO_ZONA_IVA_EMPRESA_FAC` AS `GRUPO_ZONA_IVA_EMPRESA_FAC`,`vi_facturas`.`ESREGIMENESPECIALAGRICOLA_EMPRESA_FAC` AS `ESREGIMENESPECIALAGRICOLA_EMPRESA_FAC`,`vi_facturas`.`CODIGO_CLI_FAC` AS `CODIGO_CLI_FAC`,`vi_facturas`.`RAZON_SOCIAL_CLIENTE_FAC` AS `RAZON_SOCIAL_CLIENTE_FAC`,`vi_facturas`.`NIF_CLIENTE_FAC` AS `NIF_CLIENTE_FAC`,`vi_facturas`.`MOVIL_CLIENTE_FAC` AS `MOVIL_CLIENTE_FAC`,`vi_facturas`.`EMAIL_CLIENTE_FAC` AS `EMAIL_CLIENTE_FAC`,`vi_facturas`.`DIRECCION1_CLIENTE_FAC` AS `DIRECCION1_CLIENTE_FAC`,`vi_facturas`.`DIRECCION2_CLIENTE_FAC` AS `DIRECCION2_CLIENTE_FAC`,`vi_facturas`.`POBLACION_CLIENTE_FAC` AS `POBLACION_CLIENTE_FAC`,`vi_facturas`.`PROVINCIA_CLIENTE_FAC` AS `PROVINCIA_CLIENTE_FAC`,`vi_facturas`.`CODIGO_POSTAL_CLIENTE_FAC` AS `CODIGO_POSTAL_CLIENTE_FAC`,`vi_facturas`.`NOMBRE_PAI_CLIENTE_FAC` AS `NOMBRE_PAI_CLIENTE_FAC`,`vi_facturas`.`CODIGO_PAI_CLIENTE_FAC` AS `CODIGO_PAI_CLIENTE_FAC`,`vi_facturas`.`ESIVA_RECARGO_CLIENTE_FAC` AS `ESIVA_RECARGO_CLIENTE_FAC`,`vi_facturas`.`ESIVA_EXENTO_CLIENTE_FAC` AS `ESIVA_EXENTO_CLIENTE_FAC`,`vi_facturas`.`ESREGIMENESPECIALAGRICOLA_CLIENTE_FAC` AS `ESREGIMENESPECIALAGRICOLA_CLIENTE_FAC`,`vi_facturas`.`ESRETENCIONES_CLIENTE_FAC` AS `ESRETENCIONES_CLIENTE_FAC`,`vi_facturas`.`TARIFA_ARTICULO_CLIENTE_FAC` AS `TARIFA_ARTICULO_CLIENTE_FAC`,`vi_facturas`.`ESIMP_INCL_TARIFA_CLIENTE_FAC` AS `ESIMP_INCL_TARIFA_CLIENTE_FAC`,`vi_facturas`.`ESINTRACOMUNITARIO_CLIENTE_FAC` AS `ESINTRACOMUNITARIO_CLIENTE_FAC`,`vi_facturas`.`ESIRPF_IMP_INCL_ZONA_IVA_FAC` AS `ESIRPF_IMP_INCL_ZONA_IVA_FAC`,`vi_facturas`.`ESAPLICA_RE_ZONA_IVA_FAC` AS `ESAPLICA_RE_ZONA_IVA_FAC`,`vi_facturas`.`ESIVAAGRICOLA_ZONA_IVA_FAC` AS `ESIVAAGRICOLA_ZONA_IVA_FAC`,`vi_facturas`.`PALABRA_REPORTS_ZONA_IVA_FAC` AS `PALABRA_REPORTS_ZONA_IVA_FAC`,`vi_facturas`.`CODIGO_IVA_FAC` AS `CODIGO_IVA_FAC`,`vi_facturas`.`ESVENTA_ACTIVO_FIJO_FAC` AS `ESVENTA_ACTIVO_FIJO_FAC`,`vi_facturas`.`PORCENTAJE_IVAN_FAC` AS `PORCENTAJE_IVAN_FAC`,`vi_facturas`.`TOTAL_IVAN_FAC` AS `TOTAL_IVAN_FAC`,`vi_facturas`.`PORCENTAJE_REN_FAC` AS `PORCENTAJE_REN_FAC`,`vi_facturas`.`TOTAL_REN_FAC` AS `TOTAL_REN_FAC`,`vi_facturas`.`TOTAL_BASEI_IVAN_FAC` AS `TOTAL_BASEI_IVAN_FAC`,`vi_facturas`.`PORCENTAJE_IVAR_FAC` AS `PORCENTAJE_IVAR_FAC`,`vi_facturas`.`TOTAL_IVAR_FAC` AS `TOTAL_IVAR_FAC`,`vi_facturas`.`PORCENTAJE_RER_FAC` AS `PORCENTAJE_RER_FAC`,`vi_facturas`.`TOTAL_RER_FAC` AS `TOTAL_RER_FAC`,`vi_facturas`.`TOTAL_BASEI_IVAR_FAC` AS `TOTAL_BASEI_IVAR_FAC`,`vi_facturas`.`PORCENTAJE_IVAS_FAC` AS `PORCENTAJE_IVAS_FAC`,`vi_facturas`.`TOTAL_IVAS_FAC` AS `TOTAL_IVAS_FAC`,`vi_facturas`.`PORCENTAJE_RES_FAC` AS `PORCENTAJE_RES_FAC`,`vi_facturas`.`TOTAL_RES_FAC` AS `TOTAL_RES_FAC`,`vi_facturas`.`TOTAL_BASEI_IVAS_FAC` AS `TOTAL_BASEI_IVAS_FAC`,`vi_facturas`.`PORCENTAJE_IVAE_FAC` AS `PORCENTAJE_IVAE_FAC`,`vi_facturas`.`TOTAL_IVAE_FAC` AS `TOTAL_IVAE_FAC`,`vi_facturas`.`PORCENTAJE_REE_FAC` AS `PORCENTAJE_REE_FAC`,`vi_facturas`.`TOTAL_REE_FAC` AS `TOTAL_REE_FAC`,`vi_facturas`.`TOTAL_BASEI_IVAE_FAC` AS `TOTAL_BASEI_IVAE_FAC`,`vi_facturas`.`NUMERO_FAC_ABONO_FAC` AS `NUMERO_FAC_ABONO_FAC`,`vi_facturas`.`SERIE_FAC_ABONO_FAC` AS `SERIE_FAC_ABONO_FAC`,`vi_facturas`.`TEXTO_LEGAL_CLIENTE_FAC` AS `TEXTO_LEGAL_CLIENTE_FAC`,`vi_facturas`.`TEXTO_LEGAL_EMPRESA_FAC` AS `TEXTO_LEGAL_EMPRESA_FAC`,`vi_facturas`.`DOCUMENTO_FAC` AS `DOCUMENTO_FAC`,`vi_facturas`.`XML_FAC` AS `XML_FAC`,`vi_facturas`.`COMENTARIOS_FAC` AS `COMENTARIOS_FAC`,`vi_facturas`.`CONTADOR_LINEAS_FAC` AS `CONTADOR_LINEAS_FAC`,`vi_facturas`.`ESCREARARTICULOS_FAC` AS `ESCREARARTICULOS_FAC`,`vi_facturas`.`ESDESCRIPCIONES_AMP_FAC` AS `ESDESCRIPCIONES_AMP_FAC`,`vi_facturas`.`ESFECHADEENTREGA_FAC` AS `ESFECHADEENTREGA_FAC`,`vi_facturas`.`CODIGO_ALM_FAC` AS `CODIGO_ALM_FAC`,`vi_facturas`.`CODIGO_CAJA_FAC` AS `CODIGO_CAJA_FAC`,`vi_facturas`.`NUMERO_OPERACION_FAC` AS `NUMERO_OPERACION_FAC`,`vi_facturas`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`vi_facturas`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`vi_facturas`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`vi_facturas`.`USUARIO_MODIF` AS `USUARIO_MODIF`,`vi_facturas`.`DESCRIPCION_FORMA_PAGO_FP` AS `DESCRIPCION_FORMA_PAGO_FP`,`vi_facturas`.`ESCONSOLIDADA_FAC` AS `ESCONSOLIDADA_FAC`,`vi_facturas`.`INSTANTECONSO_FAC` AS `INSTANTECONSO_FAC` from `vi_facturas` where `vi_facturas`.`TIPO_FAC` = 'SIMPLIFICADA';

-- Recreando vista: vi_fac_busquedas
DROP VIEW IF EXISTS `vi_fac_busquedas`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_fac_busquedas` AS select `fza_facturas`.`FECHA_FAC` AS `FECHA_FAC`,`fza_facturas`.`NUMERO_FAC` AS `NUMERO_FAC`,`fza_facturas`.`SERIE_FAC` AS `SERIE_FAC`,`fza_facturas`.`TOTAL_LIQUIDO_FAC` AS `TOTAL_LIQUIDO_FAC`,`fza_facturas`.`PORCENTAJE_RETENCION_FAC` AS `PORCENTAJE_RETENCION_FAC`,`fza_facturas`.`TOTAL_RETENCION_FAC` AS `TOTAL_RETENCION_FAC`,`fza_facturas`.`TOTAL_IMPUESTOS_FAC` AS `TOTAL_IMPUESTOS_FAC`,`fza_facturas`.`TOTAL_BASES_FAC` AS `TOTAL_BASES_FAC`,`fza_facturas`.`FORMA_PAGO_FAC` AS `FORMA_PAGO_FAC`,`fza_formas_pago`.`DESCRIPCION_FORMA_PAGO_FP` AS `DESCRIPCION_FORMA_PAGO_FP`,`fza_facturas`.`CODIGO_EMP_FAC` AS `CODIGO_EMP_FAC`,`fza_facturas`.`RAZON_SOCIAL_EMPRESA_FAC` AS `RAZON_SOCIAL_EMPRESA_FAC`,`fza_facturas`.`NIF_EMPRESA_FAC` AS `NIF_EMPRESA_FAC`,`fza_facturas`.`MOVIL_EMPRESA_FAC` AS `MOVIL_EMPRESA_FAC`,`fza_facturas`.`EMAIL_EMPRESA_FAC` AS `EMAIL_EMPRESA_FAC`,`fza_facturas`.`DIRECCION1_EMPRESA_FAC` AS `DIRECCION1_EMPRESA_FAC`,`fza_facturas`.`DIRECCION2_EMPRESA_FAC` AS `DIRECCION2_EMPRESA_FAC`,`fza_facturas`.`POBLACION_EMPRESA_FAC` AS `POBLACION_EMPRESA_FAC`,`fza_facturas`.`PROVINCIA_EMPRESA_FAC` AS `PROVINCIA_EMPRESA_FAC`,`fza_facturas`.`NOMBRE_PAI_EMPRESA_FAC` AS `NOMBRE_PAI_EMPRESA_FAC`,`fza_facturas`.`CODIGO_POSTAL_EMPRESA_FAC` AS `CODIGO_POSTAL_EMPRESA_FAC`,`fza_facturas`.`ESRETENCIONES_EMPRESA_FAC` AS `ESRETENCIONES_EMPRESA_FAC`,`fza_facturas`.`GRUPO_ZONA_IVA_EMPRESA_FAC` AS `GRUPO_ZONA_IVA_EMPRESA_FAC`,`fza_facturas`.`ESREGIMENESPECIALAGRICOLA_EMPRESA_FAC` AS `ESREGIMENESPECIALAGRICOLA_EMPRESA_FAC`,`fza_facturas`.`CODIGO_CLI_FAC` AS `CODIGO_CLI_FAC`,`fza_facturas`.`RAZON_SOCIAL_CLIENTE_FAC` AS `RAZON_SOCIAL_CLIENTE_FAC`,`fza_facturas`.`NIF_CLIENTE_FAC` AS `NIF_CLIENTE_FAC`,`fza_facturas`.`MOVIL_CLIENTE_FAC` AS `MOVIL_CLIENTE_FAC`,`fza_facturas`.`EMAIL_CLIENTE_FAC` AS `EMAIL_CLIENTE_FAC`,`fza_facturas`.`DIRECCION1_CLIENTE_FAC` AS `DIRECCION1_CLIENTE_FAC`,`fza_facturas`.`DIRECCION2_CLIENTE_FAC` AS `DIRECCION2_CLIENTE_FAC`,`fza_facturas`.`POBLACION_CLIENTE_FAC` AS `POBLACION_CLIENTE_FAC`,`fza_facturas`.`PROVINCIA_CLIENTE_FAC` AS `PROVINCIA_CLIENTE_FAC`,`fza_facturas`.`CODIGO_POSTAL_CLIENTE_FAC` AS `CODIGO_POSTAL_CLIENTE_FAC`,`fza_facturas`.`NOMBRE_PAI_CLIENTE_FAC` AS `NOMBRE_PAI_CLIENTE_FAC`,`fza_facturas`.`ESIVA_RECARGO_CLIENTE_FAC` AS `ESIVA_RECARGO_CLIENTE_FAC`,`fza_facturas`.`ESIVA_EXENTO_CLIENTE_FAC` AS `ESIVA_EXENTO_CLIENTE_FAC`,`fza_facturas`.`ESREGIMENESPECIALAGRICOLA_CLIENTE_FAC` AS `ESREGIMENESPECIALAGRICOLA_CLIENTE_FAC`,`fza_facturas`.`ESRETENCIONES_CLIENTE_FAC` AS `ESRETENCIONES_CLIENTE_FAC`,`fza_facturas`.`TARIFA_ARTICULO_CLIENTE_FAC` AS `TARIFA_ARTICULO_CLIENTE_FAC`,`fza_facturas`.`ESIMP_INCL_TARIFA_CLIENTE_FAC` AS `ESIMP_INCL_TARIFA_CLIENTE_FAC`,`fza_facturas`.`ESINTRACOMUNITARIO_CLIENTE_FAC` AS `ESINTRACOMUNITARIO_CLIENTE_FAC`,`fza_facturas`.`ESIRPF_IMP_INCL_ZONA_IVA_FAC` AS `ESIRPF_IMP_INCL_ZONA_IVA_FAC`,`fza_facturas`.`ESAPLICA_RE_ZONA_IVA_FAC` AS `ESAPLICA_RE_ZONA_IVA_FAC`,`fza_facturas`.`ESIVAAGRICOLA_ZONA_IVA_FAC` AS `ESIVAAGRICOLA_ZONA_IVA_FAC`,`fza_facturas`.`PALABRA_REPORTS_ZONA_IVA_FAC` AS `PALABRA_REPORTS_ZONA_IVA_FAC`,`fza_facturas`.`CODIGO_IVA_FAC` AS `CODIGO_IVA_FAC`,`fza_facturas`.`ESVENTA_ACTIVO_FIJO_FAC` AS `ESVENTA_ACTIVO_FIJO_FAC`,`fza_facturas`.`PORCENTAJE_IVAN_FAC` AS `PORCENTAJE_IVAN_FAC`,`fza_facturas`.`TOTAL_IVAN_FAC` AS `TOTAL_IVAN_FAC`,`fza_facturas`.`PORCENTAJE_REN_FAC` AS `PORCENTAJE_REN_FAC`,`fza_facturas`.`TOTAL_REN_FAC` AS `TOTAL_REN_FAC`,`fza_facturas`.`TOTAL_BASEI_IVAN_FAC` AS `TOTAL_BASEI_IVAN_FAC`,`fza_facturas`.`PORCENTAJE_IVAR_FAC` AS `PORCENTAJE_IVAR_FAC`,`fza_facturas`.`TOTAL_IVAR_FAC` AS `TOTAL_IVAR_FAC`,`fza_facturas`.`PORCENTAJE_RER_FAC` AS `PORCENTAJE_RER_FAC`,`fza_facturas`.`TOTAL_RER_FAC` AS `TOTAL_RER_FAC`,`fza_facturas`.`TOTAL_BASEI_IVAR_FAC` AS `TOTAL_BASEI_IVAR_FAC`,`fza_facturas`.`PORCENTAJE_IVAS_FAC` AS `PORCENTAJE_IVAS_FAC`,`fza_facturas`.`TOTAL_IVAS_FAC` AS `TOTAL_IVAS_FAC`,`fza_facturas`.`PORCENTAJE_RES_FAC` AS `PORCENTAJE_RES_FAC`,`fza_facturas`.`TOTAL_RES_FAC` AS `TOTAL_RES_FAC`,`fza_facturas`.`TOTAL_BASEI_IVAS_FAC` AS `TOTAL_BASEI_IVAS_FAC`,`fza_facturas`.`PORCENTAJE_IVAE_FAC` AS `PORCENTAJE_IVAE_FAC`,`fza_facturas`.`TOTAL_IVAE_FAC` AS `TOTAL_IVAE_FAC`,`fza_facturas`.`PORCENTAJE_REE_FAC` AS `PORCENTAJE_REE_FAC`,`fza_facturas`.`TOTAL_REE_FAC` AS `TOTAL_REE_FAC`,`fza_facturas`.`TOTAL_BASEI_IVAE_FAC` AS `TOTAL_BASEI_IVAE_FAC` from (`fza_facturas` left join `fza_formas_pago` on(`fza_facturas`.`FORMA_PAGO_FAC` = `fza_formas_pago`.`CODIGO_FP_FP`));

-- Recreando vista: vi_fac_lin_busquedas
DROP VIEW IF EXISTS `vi_fac_lin_busquedas`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_fac_lin_busquedas` AS select `fza_facturas_lineas`.`NUMERO_FAC_FACLIN` AS `NUMERO_FAC_FACLIN`,`fza_facturas_lineas`.`SERIE_FAC_FACLIN` AS `SERIE_FAC_FACLIN`,`fza_facturas_lineas`.`LINEA_FACLIN` AS `LINEA_FACLIN`,`fza_facturas_lineas`.`CODIGO_ART_FACLIN` AS `CODIGO_ART_FACLIN`,`fza_facturas_lineas`.`CODIGO_FAM_FACLIN` AS `CODIGO_FAM_FACLIN`,`fza_facturas_lineas`.`NOMBRE_FAM_FACLIN` AS `NOMBRE_FAM_FACLIN`,`fza_facturas_lineas`.`FECHA_ENTREGA_FACLIN` AS `FECHA_ENTREGA_FACLIN`,`fza_facturas_lineas`.`TIPO_CANTIDAD_ARTICULO_FACLIN` AS `TIPO_CANTIDAD_ARTICULO_FACLIN`,`fza_facturas_lineas`.`ESIMP_INCL_TARIFA_FACLIN` AS `ESIMP_INCL_TARIFA_FACLIN`,`fza_facturas_lineas`.`TIPO_IVA_ARTICULO_FACLIN` AS `TIPO_IVA_ARTICULO_FACLIN`,`fza_ivas_tipos`.`NOMBRE_TIPO_IVA_IVATIP` AS `NOMBRE_TIPO_IVA_IVATIP`,`fza_facturas_lineas`.`DESCRIPCION_ARTICULO_FACLIN` AS `DESCRIPCION_ARTICULO_FACLIN`,`fza_facturas_lineas`.`CODIGO_TAR_FACLIN` AS `CODIGO_TAR_FACLIN`,`fza_facturas_lineas`.`CANTIDAD_FACLIN` AS `CANTIDAD_FACLIN`,`fza_facturas_lineas`.`PRECIO_SALIDA_FACLIN` AS `PRECIO_SALIDA_FACLIN`,`fza_facturas_lineas`.`PORCENTAJE_DTO_FACLIN` AS `PORCENTAJE_DTO_FACLIN`,`fza_facturas_lineas`.`PRECIO_DTO_FACLIN` AS `PRECIO_DTO_FACLIN`,`fza_facturas_lineas`.`PRECIO_VENTA_SIVA_ARTICULO_FACLIN` AS `PRECIO_VENTA_SIVA_ARTICULO_FACLIN`,`fza_facturas_lineas`.`PORCENTAJE_IVA_FACLIN` AS `PORCENTAJE_IVA_FACLIN`,`fza_facturas_lineas`.`PRECIO_VENTA_CIVA_ARTICULO_FACLIN` AS `PRECIO_VENTA_CIVA_ARTICULO_FACLIN`,`fza_facturas_lineas`.`TOTAL_FACLIN` AS `TOTAL_FACLIN`,`fza_facturas_lineas`.`TOTAL_FAC_SIVA_FACLIN` AS `TOTAL_FAC_SIVA_FACLIN`,`fza_facturas`.`CODIGO_CLI_FAC` AS `CODIGO_CLIENTE_FACTURA_LINEA`,`fza_facturas`.`CODIGO_EMP_FAC` AS `CODIGO_EMP_FACLIN` from ((`fza_facturas_lineas` left join `fza_facturas` on(`fza_facturas_lineas`.`NUMERO_FAC_FACLIN` = `fza_facturas`.`NUMERO_FAC` and `fza_facturas_lineas`.`SERIE_FAC_FACLIN` = `fza_facturas`.`SERIE_FAC`)) left join `fza_ivas_tipos` on(`fza_facturas_lineas`.`TIPO_IVA_ARTICULO_FACLIN` = `fza_ivas_tipos`.`CODIGO_ABREVIATURA_IVA_IVATIP`));

-- Recreando vista: vi_formapago
DROP VIEW IF EXISTS `vi_formapago`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_formapago` AS select `fza_formas_pago`.`CODIGO_FP_FP` AS `CODIGO_FP_FP`,`fza_formas_pago`.`ESACTIVO_FORMA_PAGO_FP` AS `ESACTIVO_FORMA_PAGO_FP`,`fza_formas_pago`.`ORDEN_FORMA_PAGO_FP` AS `ORDEN_FORMA_PAGO_FP`,`fza_formas_pago`.`DESCRIPCION_FORMA_PAGO_FP` AS `DESCRIPCION_FORMA_PAGO_FP`,`fza_formas_pago`.`N_PLAZOS_FORMA_PAGO_FP` AS `N_PLAZOS_FORMA_PAGO_FP`,`fza_formas_pago`.`N_DIAS_ENTRE_PLAZOS_FORMA_PAGO_FP` AS `N_DIAS_ENTRE_PLAZOS_FORMA_PAGO_FP`,`fza_formas_pago`.`PORCENTAJE_ANTICIPO_FORMA_PAGO_FP` AS `PORCENTAJE_ANTICIPO_FORMA_PAGO_FP`,`fza_formas_pago`.`ESVERBANCOEMPRESA_FORMA_PAGO_FP` AS `ESVERBANCOEMPRESA_FORMA_PAGO_FP`,`fza_formas_pago`.`ESDEFAULT_FORMA_PAGO_FP` AS `ESDEFAULT_FORMA_PAGO_FP`,`fza_formas_pago`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_formas_pago`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_formas_pago`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_formas_pago`.`USUARIO_MODIF` AS `USUARIO_MODIF` from `fza_formas_pago`;

-- Recreando vista: vi_info_tpv_completa
DROP VIEW IF EXISTS `vi_info_tpv_completa`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_info_tpv_completa` AS select `a`.`CODIGO_ART_ART` AS `CODIGO_ART_ART`,`a`.`DESCRIPCION_ART` AS `DESCRIPCION_ART`,`a`.`TIPO_ART` AS `TIPO_ART`,`a`.`ESTRAZABLE_ART` AS `TIENE_TRAZABILIDAD`,(select count(0) from `vi_atributos_nombres` `n` where `n`.`CODIGO_ART_PADRE_ARTVIN` = `a`.`CODIGO_ART_ART`) AS `NUM_ATRIBUTOS_REQ`,(select `n`.`NOMBRE_ATRIBUTO` from `vi_atributos_nombres` `n` where `n`.`CODIGO_ART_PADRE_ARTVIN` = `a`.`CODIGO_ART_ART` and `n`.`ORDEN_VISUAL_ATRIBUTO` = 1 limit 1) AS `ATTR1_NOMBRE`,(select `n`.`NOMBRE_ATRIBUTO` from `vi_atributos_nombres` `n` where `n`.`CODIGO_ART_PADRE_ARTVIN` = `a`.`CODIGO_ART_ART` and `n`.`ORDEN_VISUAL_ATRIBUTO` = 2 limit 1) AS `ATTR2_NOMBRE`,(select `n`.`NOMBRE_ATRIBUTO` from `vi_atributos_nombres` `n` where `n`.`CODIGO_ART_PADRE_ARTVIN` = `a`.`CODIGO_ART_ART` and `n`.`ORDEN_VISUAL_ATRIBUTO` = 3 limit 1) AS `ATTR3_NOMBRE`,(select `n`.`NOMBRE_ATRIBUTO` from `vi_atributos_nombres` `n` where `n`.`CODIGO_ART_PADRE_ARTVIN` = `a`.`CODIGO_ART_ART` and `n`.`ORDEN_VISUAL_ATRIBUTO` = 4 limit 1) AS `ATTR4_NOMBRE`,(select `n`.`NOMBRE_ATRIBUTO` from `vi_atributos_nombres` `n` where `n`.`CODIGO_ART_PADRE_ARTVIN` = `a`.`CODIGO_ART_ART` and `n`.`ORDEN_VISUAL_ATRIBUTO` = 5 limit 1) AS `ATTR5_NOMBRE` from `fza_articulos` `a`;

-- Recreando vista: vi_ivas
DROP VIEW IF EXISTS `vi_ivas`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_ivas` AS select `fza_ivas`.`CODIGO_IVA` AS `CODIGO_IVA`,`fza_ivas`.`IVA_IVAGRP` AS `IVA_IVAGRP`,`fza_ivas`.`DESCRIPCION_IVA_IVAGRP` AS `DESCRIPCION_IVA_IVAGRP`,`fza_ivas`.`PORCENTAJE_EXENTO_IVA` AS `PORCENTAJE_EXENTO_IVA`,`fza_ivas`.`PORCENTAJE_EXENTO_RE_IVA` AS `PORCENTAJE_EXENTO_RE_IVA`,`fza_ivas`.`PORCENTAJE_NORMAL_IVA` AS `PORCENTAJE_NORMAL_IVA`,`fza_ivas`.`PORCENTAJE_NORMAL_RE_IVA` AS `PORCENTAJE_NORMAL_RE_IVA`,`fza_ivas`.`PORCENTAJE_REDUCIDO_IVA` AS `PORCENTAJE_REDUCIDO_IVA`,`fza_ivas`.`PORCENTAJE_REDUCIDO_RE_IVA` AS `PORCENTAJE_REDUCIDO_RE_IVA`,`fza_ivas`.`PORCENTAJE_SUPERREDUCIDO_IVA` AS `PORCENTAJE_SUPERREDUCIDO_IVA`,`fza_ivas`.`PORCENTAJE_SUPERREDUCIDO_RE_IVA` AS `PORCENTAJE_SUPERREDUCIDO_RE_IVA`,`fza_ivas`.`FECHA_DESDE_IVA` AS `FECHA_DESDE_IVA`,`fza_ivas`.`FECHA_HASTA_IVA` AS `FECHA_HASTA_IVA`,`fza_ivas`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_ivas`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_ivas`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_ivas`.`USUARIO_MODIF` AS `USUARIO_MODIF`,`fza_ivas_grupos`.`ESAPLICA_RE_IVA_IVAGRP` AS `ESAPLICA_RE_IVA_IVAGRP`,`fza_ivas_grupos`.`ESIVAAGRICOLA_IVA_IVAGRP` AS `ESIVAAGRICOLA_IVA_IVAGRP`,`fza_ivas_grupos`.`ESDEFAULT_IVA_IVAGRP` AS `ESDEFAULT_IVA_IVAGRP`,`fza_ivas_grupos`.`ESIRPF_IMP_INCL_IVA_IVAGRP` AS `ESIRPF_IMP_INCL_IVA_IVAGRP`,`fza_ivas_grupos`.`PALABRA_REPORTS_IVA_IVAGRP` AS `PALABRA_REPORTS_IVA_IVAGRP` from (`fza_ivas` join `fza_ivas_grupos` on(`fza_ivas`.`IVA_IVAGRP` = `fza_ivas_grupos`.`IVA_IVAGRP`));

-- Recreando vista: vi_ivas_empresa
DROP VIEW IF EXISTS `vi_ivas_empresa`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_ivas_empresa` AS select `i`.`CODIGO_IVA` AS `CODIGO_IVA`,`i`.`IVA_IVAGRP` AS `IVA_IVAGRP`,`i`.`DESCRIPCION_IVA_IVAGRP` AS `DESCRIPCION_IVA_IVAGRP`,`i`.`PORCENTAJE_EXENTO_IVA` AS `PORCENTAJE_EXENTO_IVA`,`i`.`PORCENTAJE_EXENTO_RE_IVA` AS `PORCENTAJE_EXENTO_RE_IVA`,`i`.`PORCENTAJE_NORMAL_IVA` AS `PORCENTAJE_NORMAL_IVA`,`i`.`PORCENTAJE_NORMAL_RE_IVA` AS `PORCENTAJE_NORMAL_RE_IVA`,`i`.`PORCENTAJE_REDUCIDO_IVA` AS `PORCENTAJE_REDUCIDO_IVA`,`i`.`PORCENTAJE_REDUCIDO_RE_IVA` AS `PORCENTAJE_REDUCIDO_RE_IVA`,`i`.`PORCENTAJE_SUPERREDUCIDO_IVA` AS `PORCENTAJE_SUPERREDUCIDO_IVA`,`i`.`PORCENTAJE_SUPERREDUCIDO_RE_IVA` AS `PORCENTAJE_SUPERREDUCIDO_RE_IVA`,`i`.`FECHA_DESDE_IVA` AS `FECHA_DESDE_IVA`,`i`.`FECHA_HASTA_IVA` AS `FECHA_HASTA_IVA`,`ig`.`ESAPLICA_RE_IVA_IVAGRP` AS `ESAPLICA_RE_IVA_IVAGRP`,`ig`.`ESIVAAGRICOLA_IVA_IVAGRP` AS `ESIVAAGRICOLA_IVA_IVAGRP`,`ig`.`ESDEFAULT_IVA_IVAGRP` AS `ESDEFAULT_IVA_IVAGRP`,`em`.`CODIGO_EMP_EMP` AS `CODIGO_EMP_EMP`,`ig`.`ESIRPF_IMP_INCL_IVA_IVAGRP` AS `ESIRPF_IMP_INCL_IVA_IVAGRP`,`ig`.`PALABRA_REPORTS_IVA_IVAGRP` AS `PALABRA_REPORTS_IVA_IVAGRP` from ((`fza_ivas` `i` join `fza_ivas_grupos` `ig` on(`i`.`IVA_IVAGRP` = `ig`.`IVA_IVAGRP`)) join `fza_empresas` `em` on(`em`.`GRUPO_ZONA_IVA_EMP` = `ig`.`IVA_IVAGRP`));

-- Recreando vista: vi_ivas_grupos
DROP VIEW IF EXISTS `vi_ivas_grupos`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_ivas_grupos` AS select `fza_ivas_grupos`.`IVA_IVAGRP` AS `IVA_IVAGRP`,`fza_ivas_grupos`.`DESCRIPCION_IVA_IVAGRP` AS `DESCRIPCION_IVA_IVAGRP`,`fza_ivas_grupos`.`ESIRPF_IMP_INCL_IVA_IVAGRP` AS `ESIRPF_IMP_INCL_IVA_IVAGRP`,`fza_ivas_grupos`.`ESIVAAGRICOLA_IVA_IVAGRP` AS `ESIVAAGRICOLA_IVA_IVAGRP`,`fza_ivas_grupos`.`ESAPLICA_RE_IVA_IVAGRP` AS `ESAPLICA_RE_IVA_IVAGRP`,`fza_ivas_grupos`.`ESDEFAULT_IVA_IVAGRP` AS `ESDEFAULT_IVA_IVAGRP`,`fza_ivas_grupos`.`PALABRA_REPORTS_IVA_IVAGRP` AS `PALABRA_REPORTS_IVA_IVAGRP`,`fza_ivas_grupos`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_ivas_grupos`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_ivas_grupos`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_ivas_grupos`.`USUARIO_MODIF` AS `USUARIO_MODIF` from `fza_ivas_grupos`;

-- Recreando vista: vi_ivas_zonas
DROP VIEW IF EXISTS `vi_ivas_zonas`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_ivas_zonas` AS select `fza_ivas_zonas`.`CODIGO_ZONA_IVA_IVAZON` AS `CODIGO_ZONA_IVA_IVAZON`,`fza_ivas_zonas`.`DESCRIPCION_IVA_IVAGRP` AS `DESCRIPCION_IVA_IVAGRP`,`fza_ivas_zonas`.`ESDEFAULT_IVA_IVAGRP` AS `ESDEFAULT_IVA_IVAGRP` from `fza_ivas_zonas`;

-- Recreando vista: vi_movimientos
DROP VIEW IF EXISTS `vi_movimientos`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_movimientos` AS select `m`.`NUMERO_MOV` AS `NUMERO_MOV`,`m`.`TIPO_DOC_MOV` AS `TIPO_DOC_MOV`,`m`.`SERIE_DOC_MOV` AS `SERIE_DOC_MOV`,`m`.`NUMERO_DOC_MOV` AS `NUMERO_DOC_MOV`,`m`.`LINEA_MOV` AS `LINEA_MOV`,`m`.`CODIGO_EMP_MOV` AS `CODIGO_EMP_MOV`,`m`.`CODIGO_ALM_MOV` AS `CODIGO_ALM_MOV`,`m`.`FECHA_MOV` AS `FECHA_MOV`,`m`.`CODIGO_ART_MOV` AS `CODIGO_ART_MOV`,`m`.`CODIGO_UNIDAD_MOV` AS `CODIGO_UNIDAD_MOV`,`m`.`DESCRIPCION_ARTICULO_MOV` AS `DESCRIPCION_ARTICULO_MOV`,`m`.`TIPO_MOV` AS `TIPO_MOV`,`m`.`CANTIDAD_MOV` AS `CANTIDAD_MOV`,`m`.`PRECIO_COSTE_UNITARIO_MOV` AS `PRECIO_COSTE_UNITARIO_MOV`,`m`.`TOTAL_COSTE_MOV` AS `TOTAL_COSTE_MOV`,`m`.`PRECIO_MEDIO_MOV` AS `PRECIO_MEDIO_MOV`,`m`.`CODIGO_ALM_CONTRA_MOV` AS `CODIGO_ALM_CONTRA_MOV`,`m`.`CODIGO_CLI_MOV` AS `CODIGO_CLI_MOV`,`m`.`CODIGO_PRV_MOV` AS `CODIGO_PRV_MOV`,`m`.`ESACTIVO_MOV` AS `ESACTIVO_MOV`,`m`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`m`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`m`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`m`.`USUARIO_MODIF` AS `USUARIO_MODIF`,`m`.`TIPO_DOC_REF_MOV` AS `TIPO_DOC_REF_MOV`,`m`.`SERIE_DOC_REF_MOV` AS `SERIE_DOC_REF_MOV`,`m`.`NUMERO_DOC_REF_MOV` AS `NUMERO_DOC_REF_MOV`,`m`.`LINEA_REF_MOV` AS `LINEA_REF_MOV`,`m`.`LOTE_MOV` AS `LOTE_MOV`,`m`.`FECHA_CADUCIDAD_MOV` AS `FECHA_CADUCIDAD_MOV`,`ao`.`NOMBRE_ALM_ALM` AS `NOMBRE_ALMACEN_ORIGEN`,`ad`.`NOMBRE_ALM_ALM` AS `NOMBRE_ALMACEN_DESTINO`,`td`.`DESCRIPCION_TIPO_DOCUMENTO_TD` AS `DESCRIPCION_TIPO_DOCUMENTO_TD`,`c`.`RAZON_SOCIAL_CLI` AS `RAZON_SOCIAL_CLI`,`p`.`RAZON_SOCIAL_PRV` AS `RAZON_SOCIAL_PRV` from (((((`fza_movimientos_almacen` `m` left join `fza_almacenes` `ao` on(`m`.`CODIGO_ALM_MOV` = `ao`.`CODIGO_ALM_ALM`)) left join `fza_almacenes` `ad` on(`m`.`CODIGO_ALM_CONTRA_MOV` = `ad`.`CODIGO_ALM_ALM`)) left join `fza_tipos_documentos` `td` on(`m`.`TIPO_DOC_MOV` = `td`.`CODIGO_TIPO_DOCUMENTO_TD`)) left join `fza_clientes` `c` on(`m`.`CODIGO_CLI_MOV` = `c`.`CODIGO_CLI_CLI`)) left join `fza_proveedores` `p` on(`m`.`CODIGO_PRV_MOV` = `p`.`CODIGO_PRV_PRV`)) order by `m`.`FECHA_MOV`,`m`.`NUMERO_MOV`;

-- Recreando vista: vi_paises
DROP VIEW IF EXISTS `vi_paises`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_paises` AS select `fza_paises`.`COD_ALPHA2_PAI` AS `CODIGO`,`fza_paises`.`NOMBRE_SPA_PAI` AS `NOMBRE` from `fza_paises` order by `fza_paises`.`ORDEN_PAI`,`fza_paises`.`NOMBRE_SPA_PAI`;

-- Recreando vista: vi_pedidos
DROP VIEW IF EXISTS `vi_pedidos`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_pedidos` AS select `fza_pedidos`.`NUMERO_PED` AS `NUMERO_PED`,`fza_pedidos`.`SERIE_PED` AS `SERIE_PED`,`fza_pedidos`.`FECHA_PED` AS `FECHA_PED`,`fza_pedidos`.`ESCONSOLIDADO_PED` AS `ESCONSOLIDADO_PED`,`fza_pedidos`.`ESTADO_PED` AS `ESTADO_PED`,`fza_pedidos`.`FECHA_ENTREGA_PED` AS `FECHA_ENTREGA_PED`,`fza_pedidos`.`CODIGO_EMP_PED` AS `CODIGO_EMP_PED`,`fza_pedidos`.`RAZON_SOCIAL_EMPRESA_PED` AS `RAZON_SOCIAL_EMPRESA_PED`,`fza_pedidos`.`NIF_EMPRESA_PED` AS `NIF_EMPRESA_PED`,`fza_pedidos`.`MOVIL_EMPRESA_PED` AS `MOVIL_EMPRESA_PED`,`fza_pedidos`.`EMAIL_EMPRESA_PED` AS `EMAIL_EMPRESA_PED`,`fza_pedidos`.`DIRECCION1_EMPRESA_PED` AS `DIRECCION1_EMPRESA_PED`,`fza_pedidos`.`DIRECCION2_EMPRESA_PED` AS `DIRECCION2_EMPRESA_PED`,`fza_pedidos`.`POBLACION_EMPRESA_PED` AS `POBLACION_EMPRESA_PED`,`fza_pedidos`.`PROVINCIA_EMPRESA_PED` AS `PROVINCIA_EMPRESA_PED`,`fza_pedidos`.`CODIGO_PAI_EMPRESA_PED` AS `CODIGO_PAI_EMPRESA_PED`,`fza_pedidos`.`NOMBRE_PAI_EMPRESA_PED` AS `NOMBRE_PAI_EMPRESA_PED`,`fza_pedidos`.`CODIGO_POSTAL_EMPRESA_PED` AS `CODIGO_POSTAL_EMPRESA_PED`,`fza_pedidos`.`ESRETENCIONES_EMPRESA_PED` AS `ESRETENCIONES_EMPRESA_PED`,`fza_pedidos`.`GRUPO_ZONA_IVA_EMPRESA_PED` AS `GRUPO_ZONA_IVA_EMPRESA_PED`,`fza_pedidos`.`ESREGIMENESPECIALAGRICOLA_EMPRESA_PED` AS `ESREGIMENESPECIALAGRICOLA_EMPRESA_PED`,`fza_pedidos`.`CODIGO_CLI_PED` AS `CODIGO_CLI_PED`,`fza_pedidos`.`NIF_CLIENTE_PED` AS `NIF_CLIENTE_PED`,`fza_pedidos`.`EMAIL_CLIENTE_PED` AS `EMAIL_CLIENTE_PED`,`fza_pedidos`.`REFERENCIAPS_PED` AS `REFERENCIAPS_PED`,`fza_pedidos`.`IDPS_PED` AS `IDPS_PED`,`fza_pedidos`.`FECHAPS_PED` AS `FECHAPS_PED`,`fza_pedidos`.`FORMAPAGOPS_PED` AS `FORMAPAGOPS_PED`,`fza_pedidos`.`TRANSPORTISTAPS_PED` AS `TRANSPORTISTAPS_PED`,`fza_pedidos`.`ESTADOPEDIDOPS_PED` AS `ESTADOPEDIDOPS_PED`,`fza_pedidos`.`ESTADOMENSAJEPS_PED` AS `ESTADOMENSAJEPS_PED`,`fza_pedidos`.`IDHILOPS_MENSAJES_PED` AS `IDHILOPS_MENSAJES_PED`,`fza_pedidos`.`NOMBRE_CLI_ENVIO_PED` AS `NOMBRE_CLI_ENVIO_PED`,`fza_pedidos`.`MOVIL_CLIENTE_ENVIO_PED` AS `MOVIL_CLIENTE_ENVIO_PED`,`fza_pedidos`.`DIRECCION1_CLIENTE_ENVIO_PED` AS `DIRECCION1_CLIENTE_ENVIO_PED`,`fza_pedidos`.`DIRECCION2_CLIENTE_ENVIO_PED` AS `DIRECCION2_CLIENTE_ENVIO_PED`,`fza_pedidos`.`POBLACION_CLIENTE_ENVIO_PED` AS `POBLACION_CLIENTE_ENVIO_PED`,`fza_pedidos`.`PROVINCIA_CLIENTE_ENVIO_PED` AS `PROVINCIA_CLIENTE_ENVIO_PED`,`fza_pedidos`.`CODIGO_POSTAL_CLIENTE_ENVIO_PED` AS `CODIGO_POSTAL_CLIENTE_ENVIO_PED`,`fza_pedidos`.`CODIGO_PAI_CLIENTE_ENVIO_PED` AS `CODIGO_PAI_CLIENTE_ENVIO_PED`,`fza_pedidos`.`NOMBRE_PAI_CLIENTE_ENVIO_PED` AS `NOMBRE_PAI_CLIENTE_ENVIO_PED`,`fza_pedidos`.`RAZON_SOCIAL_CLIENTE_FISCAL_PED` AS `RAZON_SOCIAL_CLIENTE_FISCAL_PED`,`fza_pedidos`.`MOVIL_CLIENTE_FISCAL_PED` AS `MOVIL_CLIENTE_FISCAL_PED`,`fza_pedidos`.`EMAIL_CLIENTE_FISCAL_PED` AS `EMAIL_CLIENTE_FISCAL_PED`,`fza_pedidos`.`DIRECCION1_CLIENTE_FISCAL_PED` AS `DIRECCION1_CLIENTE_FISCAL_PED`,`fza_pedidos`.`DIRECCION2_CLIENTE_FISCAL_PED` AS `DIRECCION2_CLIENTE_FISCAL_PED`,`fza_pedidos`.`POBLACION_CLIENTE_FISCAL_PED` AS `POBLACION_CLIENTE_FISCAL_PED`,`fza_pedidos`.`PROVINCIA_CLIENTE_FISCAL_PED` AS `PROVINCIA_CLIENTE_FISCAL_PED`,`fza_pedidos`.`CODIGO_POSTAL_CLIENTE_FISCAL_PED` AS `CODIGO_POSTAL_CLIENTE_FISCAL_PED`,`fza_pedidos`.`CODIGO_PAI_CLIENTE_FISCAL_PED` AS `CODIGO_PAI_CLIENTE_FISCAL_PED`,`fza_pedidos`.`NOMBRE_PAI_CLIENTE_FISCAL_PED` AS `NOMBRE_PAI_CLIENTE_FISCAL_PED`,`fza_pedidos`.`ESIVA_RECARGO_CLIENTE_PED` AS `ESIVA_RECARGO_CLIENTE_PED`,`fza_pedidos`.`ESIVA_EXENTO_CLIENTE_PED` AS `ESIVA_EXENTO_CLIENTE_PED`,`fza_pedidos`.`ESREGIMENESPECIALAGRICOLA_CLIENTE_PED` AS `ESREGIMENESPECIALAGRICOLA_CLIENTE_PED`,`fza_pedidos`.`ESRETENCIONES_CLIENTE_PED` AS `ESRETENCIONES_CLIENTE_PED`,`fza_pedidos`.`TARIFA_ARTICULO_CLIENTE_PED` AS `TARIFA_ARTICULO_CLIENTE_PED`,`fza_pedidos`.`ESIMP_INCL_TARIFA_CLIENTE_PED` AS `ESIMP_INCL_TARIFA_CLIENTE_PED`,`fza_pedidos`.`ESINTRACOMUNITARIO_CLIENTE_PED` AS `ESINTRACOMUNITARIO_CLIENTE_PED`,`fza_pedidos`.`ESIRPF_IMP_INCL_ZONA_IVA_PED` AS `ESIRPF_IMP_INCL_ZONA_IVA_PED`,`fza_pedidos`.`ESAPLICA_RE_ZONA_IVA_PED` AS `ESAPLICA_RE_ZONA_IVA_PED`,`fza_pedidos`.`ESIVAAGRICOLA_ZONA_IVA_PED` AS `ESIVAAGRICOLA_ZONA_IVA_PED`,`fza_pedidos`.`PALABRA_REPORTS_ZONA_IVA_PED` AS `PALABRA_REPORTS_ZONA_IVA_PED`,`fza_pedidos`.`CODIGO_IVA_PED` AS `CODIGO_IVA_PED`,`fza_pedidos`.`ESVENTA_ACTIVO_FIJO_PED` AS `ESVENTA_ACTIVO_FIJO_PED`,`fza_pedidos`.`PORCENTAJE_IVAN_PED` AS `PORCENTAJE_IVAN_PED`,`fza_pedidos`.`TOTAL_IVAN_PED` AS `TOTAL_IVAN_PED`,`fza_pedidos`.`PORCENTAJE_REN_PED` AS `PORCENTAJE_REN_PED`,`fza_pedidos`.`TOTAL_REN_PED` AS `TOTAL_REN_PED`,`fza_pedidos`.`TOTAL_BASEI_IVAN_PED` AS `TOTAL_BASEI_IVAN_PED`,`fza_pedidos`.`PORCENTAJE_IVAR_PED` AS `PORCENTAJE_IVAR_PED`,`fza_pedidos`.`TOTAL_IVAR_PED` AS `TOTAL_IVAR_PED`,`fza_pedidos`.`PORCENTAJE_RER_PED` AS `PORCENTAJE_RER_PED`,`fza_pedidos`.`TOTAL_RER_PED` AS `TOTAL_RER_PED`,`fza_pedidos`.`TOTAL_BASEI_IVAR_PED` AS `TOTAL_BASEI_IVAR_PED`,`fza_pedidos`.`PORCENTAJE_IVAS_PED` AS `PORCENTAJE_IVAS_PED`,`fza_pedidos`.`TOTAL_IVAS_PED` AS `TOTAL_IVAS_PED`,`fza_pedidos`.`PORCENTAJE_RES_PED` AS `PORCENTAJE_RES_PED`,`fza_pedidos`.`TOTAL_RES_PED` AS `TOTAL_RES_PED`,`fza_pedidos`.`TOTAL_BASEI_IVAS_PED` AS `TOTAL_BASEI_IVAS_PED`,`fza_pedidos`.`PORCENTAJE_IVAE_PED` AS `PORCENTAJE_IVAE_PED`,`fza_pedidos`.`TOTAL_IVAE_PED` AS `TOTAL_IVAE_PED`,`fza_pedidos`.`PORCENTAJE_REE_PED` AS `PORCENTAJE_REE_PED`,`fza_pedidos`.`TOTAL_REE_PED` AS `TOTAL_REE_PED`,`fza_pedidos`.`TOTAL_BASEI_IVAE_PED` AS `TOTAL_BASEI_IVAE_PED`,`fza_pedidos`.`TOTAL_BASES_PED` AS `TOTAL_BASES_PED`,`fza_pedidos`.`TOTAL_IMPUESTOS_PED` AS `TOTAL_IMPUESTOS_PED`,`fza_pedidos`.`FORMA_PAGO_PED` AS `FORMA_PAGO_PED`,`fza_pedidos`.`PORCENTAJE_RETENCION_PED` AS `PORCENTAJE_RETENCION_PED`,`fza_pedidos`.`TOTAL_RETENCION_PED` AS `TOTAL_RETENCION_PED`,`fza_pedidos`.`TOTAL_LIQUIDO_PED` AS `TOTAL_LIQUIDO_PED`,`fza_pedidos`.`TOTAL_PAGADOREALPS_PED` AS `TOTAL_PAGADOREALPS_PED`,`fza_pedidos`.`NUMERO_PED_ABONO_PED` AS `NUMERO_PED_ABONO_PED`,`fza_pedidos`.`SERIE_PED_ABONO_PED` AS `SERIE_PED_ABONO_PED`,`fza_pedidos`.`TEXTO_LEGAL_CLIENTE_PED` AS `TEXTO_LEGAL_CLIENTE_PED`,`fza_pedidos`.`TEXTO_LEGAL_EMPRESA_PED` AS `TEXTO_LEGAL_EMPRESA_PED`,`fza_pedidos`.`DOCUMENTO_PED` AS `DOCUMENTO_PED`,`fza_pedidos`.`COMENTARIOS_PED` AS `COMENTARIOS_PED`,`fza_pedidos`.`OBSERVACIONES_PED` AS `OBSERVACIONES_PED`,`fza_pedidos`.`CONTADOR_LINEAS_PED` AS `CONTADOR_LINEAS_PED`,`fza_pedidos`.`ESCREARARTICULOS_PED` AS `ESCREARARTICULOS_PED`,`fza_pedidos`.`ESDESCRIPCIONES_AMP_PED` AS `ESDESCRIPCIONES_AMP_PED`,`fza_pedidos`.`ESFECHADEENTREGA_PED` AS `ESFECHADEENTREGA_PED`,`fza_pedidos`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_pedidos`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_pedidos`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_pedidos`.`USUARIO_MODIF` AS `USUARIO_MODIF` from `fza_pedidos`;

-- Recreando vista: vi_pedidos_compra
DROP VIEW IF EXISTS `vi_pedidos_compra`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_pedidos_compra` AS select `p`.`NUMERO_PEDC` AS `NUMERO_PEDC`,`p`.`SERIE_PEDC` AS `SERIE_PEDC`,`p`.`FECHA_PEDC` AS `FECHA_PEDC`,`p`.`FECHA_PREVISTA_PEDC` AS `FECHA_PREVISTA_PEDC`,`p`.`ESTADO_PEDC` AS `ESTADO_PEDC`,`p`.`CODIGO_EMP_PEDC` AS `CODIGO_EMP_PEDC`,`p`.`RAZON_SOCIAL_EMPRESA_PEDC` AS `RAZON_SOCIAL_EMPRESA_PEDC`,`p`.`NIF_EMPRESA_PEDC` AS `NIF_EMPRESA_PEDC`,`p`.`MOVIL_EMPRESA_PEDC` AS `MOVIL_EMPRESA_PEDC`,`p`.`EMAIL_EMPRESA_PEDC` AS `EMAIL_EMPRESA_PEDC`,`p`.`DIRECCION1_EMPRESA_PEDC` AS `DIRECCION1_EMPRESA_PEDC`,`p`.`DIRECCION2_EMPRESA_PEDC` AS `DIRECCION2_EMPRESA_PEDC`,`p`.`POBLACION_EMPRESA_PEDC` AS `POBLACION_EMPRESA_PEDC`,`p`.`PROVINCIA_EMPRESA_PEDC` AS `PROVINCIA_EMPRESA_PEDC`,`p`.`CODIGO_PAI_EMPRESA_PEDC` AS `CODIGO_PAI_EMPRESA_PEDC`,`p`.`NOMBRE_PAI_EMPRESA_PEDC` AS `NOMBRE_PAI_EMPRESA_PEDC`,`p`.`CODIGO_POSTAL_EMPRESA_PEDC` AS `CODIGO_POSTAL_EMPRESA_PEDC`,`p`.`CODIGO_PRV_PEDC` AS `CODIGO_PRV_PEDC`,`p`.`RAZON_SOCIAL_PRV_PEDC` AS `RAZON_SOCIAL_PRV_PEDC`,`p`.`NIF_PRV_PEDC` AS `NIF_PRV_PEDC`,`p`.`MOVIL_PRV_PEDC` AS `MOVIL_PRV_PEDC`,`p`.`EMAIL_PRV_PEDC` AS `EMAIL_PRV_PEDC`,`p`.`DIRECCION1_PRV_PEDC` AS `DIRECCION1_PRV_PEDC`,`p`.`DIRECCION2_PRV_PEDC` AS `DIRECCION2_PRV_PEDC`,`p`.`POBLACION_PRV_PEDC` AS `POBLACION_PRV_PEDC`,`p`.`PROVINCIA_PRV_PEDC` AS `PROVINCIA_PRV_PEDC`,`p`.`CODIGO_PAI_PRV_PEDC` AS `CODIGO_PAI_PRV_PEDC`,`p`.`NOMBRE_PAI_PRV_PEDC` AS `NOMBRE_PAI_PRV_PEDC`,`p`.`CODIGO_POSTAL_PRV_PEDC` AS `CODIGO_POSTAL_PRV_PEDC`,`p`.`REF_PROVEEDOR_PEDC` AS `REF_PROVEEDOR_PEDC`,`p`.`CODIGO_ALM_PEDC` AS `CODIGO_ALM_PEDC`,`p`.`TRANSPORTISTA_PEDC` AS `TRANSPORTISTA_PEDC`,`p`.`CODIGO_IVA_PEDC` AS `CODIGO_IVA_PEDC`,`p`.`PORCENTAJE_IVAN_PEDC` AS `PORCENTAJE_IVAN_PEDC`,`p`.`TOTAL_IVAN_PEDC` AS `TOTAL_IVAN_PEDC`,`p`.`PORCENTAJE_IVAR_PEDC` AS `PORCENTAJE_IVAR_PEDC`,`p`.`TOTAL_IVAR_PEDC` AS `TOTAL_IVAR_PEDC`,`p`.`PORCENTAJE_IVAS_PEDC` AS `PORCENTAJE_IVAS_PEDC`,`p`.`TOTAL_IVAS_PEDC` AS `TOTAL_IVAS_PEDC`,`p`.`PORCENTAJE_IVAE_PEDC` AS `PORCENTAJE_IVAE_PEDC`,`p`.`TOTAL_IVAE_PEDC` AS `TOTAL_IVAE_PEDC`,`p`.`TOTAL_BASES_PEDC` AS `TOTAL_BASES_PEDC`,`p`.`TOTAL_IMPUESTOS_PEDC` AS `TOTAL_IMPUESTOS_PEDC`,`p`.`TOTAL_LIQUIDO_PEDC` AS `TOTAL_LIQUIDO_PEDC`,`p`.`FORMA_PAGO_PEDC` AS `FORMA_PAGO_PEDC`,`p`.`CONTADOR_LINEAS_PEDC` AS `CONTADOR_LINEAS_PEDC`,`p`.`COMENTARIOS_PEDC` AS `COMENTARIOS_PEDC`,`p`.`OBSERVACIONES_PEDC` AS `OBSERVACIONES_PEDC`,`p`.`ESPIVOTE_HORIZONTAL_PEDC` AS `ESPIVOTE_HORIZONTAL_PEDC`,`p`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`p`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`p`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`p`.`USUARIO_MODIF` AS `USUARIO_MODIF`,`p`.`ID_PV_TEMPORADA_PEDC` AS `ID_PV_TEMPORADA_PEDC`,`prv`.`NOMBRE_PRV` AS `NOMBRE_PRV_PEDC`,`emp`.`RAZON_SOCIAL_EMP` AS `RAZON_SOCIAL_EMPRESA_VIEW_PEDC`,`t`.`PV` AS `TEMPORADA_PEDC` from (((`fza_pedidos_compra` `p` left join `fza_proveedores` `prv` on(`prv`.`CODIGO_PRV_PRV` = `p`.`CODIGO_PRV_PEDC`)) left join `fza_empresas` `emp` on(`emp`.`CODIGO_EMP_EMP` = `p`.`CODIGO_EMP_PEDC`)) left join `fza_propiedades_valores` `t` on(`t`.`ID_PV_ARTPROP` = `p`.`ID_PV_TEMPORADA_PEDC` and `t`.`ID_PROP_PV` = 'TEMPORADA'));

-- Recreando vista: vi_pedidos_lineas
DROP VIEW IF EXISTS `vi_pedidos_lineas`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_pedidos_lineas` AS select `pl`.`NUMERO_PED_PEDLIN` AS `NUMERO_PED_PEDLIN`,`pl`.`SERIE_PED_PEDLIN` AS `SERIE_PED_PEDLIN`,`pl`.`LINEA_PEDLIN` AS `LINEA_PEDLIN`,`pl`.`IDLINEAPS_PEDLIN` AS `IDLINEAPS_PEDLIN`,`pl`.`IDPRODPS_PEDLIN` AS `IDPRODPS_PEDLIN`,`pl`.`CODIGOPRODPS_PEDLIN` AS `CODIGOPRODPS_PEDLIN`,`pl`.`IDATRIBPRODPS_PEDLIN` AS `IDATRIBPRODPS_PEDLIN`,`pl`.`CODBAR_ART_PEDLIN` AS `CODBAR_ART_PEDLIN`,`pl`.`CODIGO_ART_PEDLIN` AS `CODIGO_ART_PEDLIN`,`pl`.`CODIGO_FAM_PEDLIN` AS `CODIGO_FAM_PEDLIN`,`pl`.`NOMBRE_FAM_PEDLIN` AS `NOMBRE_FAM_PEDLIN`,`pl`.`FECHA_ENTREGA_PEDLIN` AS `FECHA_ENTREGA_PEDLIN`,`pl`.`TIPO_CANTIDAD_ARTICULO_PEDLIN` AS `TIPO_CANTIDAD_ARTICULO_PEDLIN`,`pl`.`ESIMP_INCL_TARIFA_PEDLIN` AS `ESIMP_INCL_TARIFA_PEDLIN`,`pl`.`TIPO_IVA_ARTICULO_PEDLIN` AS `TIPO_IVA_ARTICULO_PEDLIN`,`pl`.`DESCRIPCION_ARTICULO_PEDLIN` AS `DESCRIPCION_ARTICULO_PEDLIN`,`pl`.`CODIGO_TAR_PEDLIN` AS `CODIGO_TAR_PEDLIN`,`pl`.`CANTIDAD_PEDLIN` AS `CANTIDAD_PEDLIN`,`pl`.`CANTIDAD_ENTREGADA_PEDLIN` AS `CANTIDAD_ENTREGADA_PEDLIN`,`pl`.`CANTIDAD_PENDIENTE_PEDLIN` AS `CANTIDAD_PENDIENTE_PEDLIN`,`pl`.`ESENTREGADA_PEDLIN` AS `ESENTREGADA_PEDLIN`,`pl`.`CODIGO_ALMACEN_PEDLIN` AS `CODIGO_ALMACEN_PEDLIN`,`pl`.`PRECIO_VENTA_SIVA_ARTICULO_PEDLIN` AS `PRECIO_VENTA_SIVA_ARTICULO_PEDLIN`,`pl`.`PORCENTAJE_IVA_PEDLIN` AS `PORCENTAJE_IVA_PEDLIN`,`pl`.`PRECIO_VENTA_CIVA_ARTICULO_PEDLIN` AS `PRECIO_VENTA_CIVA_ARTICULO_PEDLIN`,`pl`.`TOTAL_PEDLIN` AS `TOTAL_PEDLIN`,`pl`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`pl`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`pl`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`pl`.`USUARIO_MODIF` AS `USUARIO_MODIF`,`pl`.`CANTIDAD_PEDLIN` - ifnull(`pl`.`CANTIDAD_ENTREGADA_PEDLIN`,0) AS `CANTIDAD_PENDIENTE_CALC_PEDLIN` from `fza_pedidos_lineas` `pl`;

-- Recreando vista: vi_proveedores
DROP VIEW IF EXISTS `vi_proveedores`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_proveedores` AS select `fza_proveedores`.`CODIGO_PRV_PRV` AS `CODIGO_PRV_PRV`,`fza_proveedores`.`ESACTIVO_PRV` AS `ESACTIVO_PRV`,`fza_proveedores`.`RAZON_SOCIAL_PRV` AS `RAZON_SOCIAL_PRV`,`fza_proveedores`.`NOMBRE_PRV` AS `NOMBRE_PRV`,`fza_proveedores`.`NIF_PRV` AS `NIF_PRV`,`fza_proveedores`.`MOVIL_PRV` AS `MOVIL_PRV`,`fza_proveedores`.`EMAIL_PRV` AS `EMAIL_PRV`,`fza_proveedores`.`DIRECCION1_PRV` AS `DIRECCION1_PRV`,`fza_proveedores`.`DIRECCION2_PRV` AS `DIRECCION2_PRV`,`fza_proveedores`.`POBLACION_PRV` AS `POBLACION_PRV`,`fza_proveedores`.`PROVINCIA_PRV` AS `PROVINCIA_PRV`,`fza_proveedores`.`CODIGO_POSTAL_PRV` AS `CODIGO_POSTAL_PRV`,`fza_proveedores`.`PAIS_PRV` AS `PAIS_PRV`,`fza_proveedores`.`OBSERVACIONES_PRV` AS `OBSERVACIONES_PRV`,`fza_proveedores`.`REFERENCIA_PRV` AS `REFERENCIA_PRV`,`fza_proveedores`.`CONTACTO_PRV` AS `CONTACTO_PRV`,`fza_proveedores`.`TELEFONO_CONTACTO_PRV` AS `TELEFONO_CONTACTO_PRV`,`fza_proveedores`.`TELEFONO_PRV` AS `TELEFONO_PRV`,`fza_proveedores`.`IBAN_PRV` AS `IBAN_PRV`,`fza_proveedores`.`ORDEN_PRV` AS `ORDEN_PRV`,`fza_proveedores`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_proveedores`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_proveedores`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_proveedores`.`USUARIO_MODIF` AS `USUARIO_MODIF` from `fza_proveedores`;

-- Recreando vista: vi_proveedores_articulos
DROP VIEW IF EXISTS `vi_proveedores_articulos`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_proveedores_articulos` AS select `fza_articulos_proveedores`.`CODIGO_PRV_AP` AS `CODIGO_PRV_PRV`,`fza_articulos_proveedores`.`CODIGO_ART_AP` AS `CODIGO_ART_ART`,`vi_articulos`.`DESCRIPCION_ART` AS `DESCRIPCION_ART`,`vi_articulos`.`CODIGO_FAM_ART` AS `CODIGO_FAM_FAM`,`vi_articulos`.`DESCRIPCION_FAM` AS `DESCRIPCION_FAM`,`vi_articulos`.`TIPO_CANTIDAD_ART` AS `TIPO_CANTIDAD_ARTICULO`,`vi_articulos`.`ESACTIVO_FIJO_ART` AS `ESACTIVO_FIJO_ART`,`fza_articulos_proveedores`.`PRECIO_ULT_COMPRA_AP` AS `PRECIO_ULT_COMPRA`,`fza_articulos_proveedores`.`FECHA_VALIDEZ_AP` AS `FECHA_VALIDEZ`,`fza_articulos_proveedores`.`ESPROVEEDORPRINCIPAL_AP` AS `ESPROVEEDORPRINCIPAL`,`fza_articulos_proveedores`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_articulos_proveedores`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_articulos_proveedores`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_articulos_proveedores`.`USUARIO_MODIF` AS `USUARIO_MODIF` from (`fza_articulos_proveedores` left join `vi_articulos` on(`fza_articulos_proveedores`.`CODIGO_ART_AP` = `vi_articulos`.`CODIGO_ART_ART`));

-- Recreando vista: vi_proveedores_busquedas
DROP VIEW IF EXISTS `vi_proveedores_busquedas`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_proveedores_busquedas` AS select `fza_proveedores`.`CODIGO_PRV_PRV` AS `CODIGO_PRV_PRV`,`fza_proveedores`.`ESACTIVO_PRV` AS `ESACTIVO_PRV`,`fza_proveedores`.`RAZON_SOCIAL_PRV` AS `RAZON_SOCIAL_PRV`,`fza_proveedores`.`NIF_PRV` AS `NIF_PRV`,`fza_proveedores`.`MOVIL_PRV` AS `MOVIL_PRV`,`fza_proveedores`.`EMAIL_PRV` AS `EMAIL_PRV`,`fza_proveedores`.`DIRECCION1_PRV` AS `DIRECCION1_PRV`,`fza_proveedores`.`DIRECCION2_PRV` AS `DIRECCION2_PRV`,`fza_proveedores`.`POBLACION_PRV` AS `POBLACION_PRV`,`fza_proveedores`.`PROVINCIA_PRV` AS `PROVINCIA_PRV`,`fza_proveedores`.`CODIGO_POSTAL_PRV` AS `CODIGO_POSTAL_PRV`,`fza_proveedores`.`PAIS_PRV` AS `PAIS_PRV`,`fza_proveedores`.`OBSERVACIONES_PRV` AS `OBSERVACIONES_PRV`,`fza_proveedores`.`REFERENCIA_PRV` AS `REFERENCIA_PRV`,`fza_proveedores`.`CONTACTO_PRV` AS `CONTACTO_PRV`,`fza_proveedores`.`TELEFONO_CONTACTO_PRV` AS `TELEFONO_CONTACTO_PRV`,`fza_proveedores`.`TELEFONO_PRV` AS `TELEFONO_PRV`,`fza_proveedores`.`IBAN_PRV` AS `IBAN_PRV`,`fza_proveedores`.`ORDEN_PRV` AS `ORDEN_PRV`,`fza_proveedores`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_proveedores`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_proveedores`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_proveedores`.`USUARIO_MODIF` AS `USUARIO_MODIF` from `fza_proveedores` where `fza_proveedores`.`ESACTIVO_PRV` = 'S' order by `fza_proveedores`.`ORDEN_PRV`;

-- Recreando vista: vi_recibos
DROP VIEW IF EXISTS `vi_recibos`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_recibos` AS select `fza_recibos`.`NUMERO_FAC_REC` AS `NUMERO_FAC_REC`,`fza_recibos`.`SERIE_FAC_REC` AS `SERIE_FAC_REC`,`fza_recibos`.`NUMERO_PLAZO_REC` AS `NUMERO_PLAZO_REC`,`fza_recibos`.`FORMA_PAGO_ORIGEN_RECIBO_REC` AS `FORMA_PAGO_ORIGEN_RECIBO_REC`,`fza_recibos`.`FORMA_PAGO_DESCRIPCION_ORIGEN_RECIBO_REC` AS `FORMA_PAGO_DESCRIPCION_ORIGEN_RECIBO_REC`,`fza_recibos`.`EUROS_RECIBO_REC` AS `EUROS_RECIBO_REC`,`fza_recibos`.`ESTADO_RECIBO_REC` AS `ESTADO_RECIBO_REC`,`fza_recibos`.`FECHA_EXPEDICION_RECIBO_REC` AS `FECHA_EXPEDICION_RECIBO_REC`,`fza_recibos`.`FECHA_VENCIMIENTO_RECIBO_REC` AS `FECHA_VENCIMIENTO_RECIBO_REC`,`fza_recibos`.`IBAN_CLI_REC` AS `IBAN_CLI_REC`,`fza_recibos`.`FECHA_PAGO_RECIBO_REC` AS `FECHA_PAGO_RECIBO_REC`,`fza_recibos`.`LOCALIDAD_EXPEDICION_RECIBO_REC` AS `LOCALIDAD_EXPEDICION_RECIBO_REC`,`fza_recibos`.`CODIGO_CLI_REC` AS `CODIGO_CLI_REC`,`fza_recibos`.`RAZON_SOCIAL_CLI_REC` AS `RAZON_SOCIAL_CLI_REC`,`fza_recibos`.`DIRECCION1_CLIENTE_RECIBO_REC` AS `DIRECCION1_CLIENTE_RECIBO_REC`,`fza_recibos`.`POBLACION_CLI_REC` AS `POBLACION_CLI_REC`,`fza_recibos`.`PROVINCIA_CLI_REC` AS `PROVINCIA_CLI_REC`,`fza_recibos`.`CODIGO_POSTAL_CLI_REC` AS `CODIGO_POSTAL_CLI_REC`,`fza_recibos`.`IMPORTE_LETRA_RECIBO_REC` AS `IMPORTE_LETRA_RECIBO_REC`,`fza_recibos`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_recibos`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_recibos`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_recibos`.`USUARIO_MODIF` AS `USUARIO_MODIF` from `fza_recibos` order by `fza_recibos`.`SERIE_FAC_REC`,`fza_recibos`.`NUMERO_FAC_REC`,`fza_recibos`.`NUMERO_PLAZO_REC`;

-- Recreando vista: vi_ses_preview_skus
DROP VIEW IF EXISTS `vi_ses_preview_skus`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_ses_preview_skus` AS select `l`.`SERIE_SES_SESLIN` AS `SERIE`,`l`.`NUMERO_SES_SESLIN` AS `NUMERO`,`l`.`LINEA_SESLIN` AS `LINEA`,`l`.`CODIGO_ART_TENTATIVO_SESLIN` AS `CODIGO_ART`,`l`.`DESCRIPCION_SESLIN` AS `DESCRIPCION`,`c`.`ID_FILA_SES_SESCEL` AS `ID_FILA`,`c`.`ID_AV_PIVOT_SESCEL` AS `ID_AV_PIVOT`,`avp`.`AV` AS `VALOR_PIVOT`,if(`c`.`CODIGO_ALM_SESCEL` = '',`s`.`CODIGO_ALM_SES`,`c`.`CODIGO_ALM_SESCEL`) AS `CODIGO_ALM`,`c`.`CANTIDAD_SESCEL` AS `CANTIDAD`,`l`.`PRECIO_COMPRA_SESLIN` AS `PRECIO_COMPRA`,`l`.`PRECIO_VENTA_SESLIN` AS `PRECIO_VENTA` from (((`fza_compras_sesiones_lineas` `l` join `fza_compras_sesiones` `s` on(`s`.`SERIE_SES` = `l`.`SERIE_SES_SESLIN` and `s`.`NUMERO_SES` = `l`.`NUMERO_SES_SESLIN`)) join `fza_compras_sesiones_celdas` `c` on(`c`.`SERIE_SES_SESCEL` = `l`.`SERIE_SES_SESLIN` and `c`.`NUMERO_SES_SESCEL` = `l`.`NUMERO_SES_SESLIN` and `c`.`LINEA_SES_SESCEL` = `l`.`LINEA_SESLIN`)) join `fza_atributos_valores` `avp` on(`avp`.`ID_AV` = `c`.`ID_AV_PIVOT_SESCEL`)) where `c`.`CANTIDAD_SESCEL` > 0;

-- Recreando vista: vi_ses_resumen
DROP VIEW IF EXISTS `vi_ses_resumen`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_ses_resumen` AS select `s`.`SERIE_SES` AS `SERIE_SES`,`s`.`NUMERO_SES` AS `NUMERO_SES`,`s`.`FECHA_SES` AS `FECHA_SES`,`s`.`CODIGO_PRV_SES` AS `CODIGO_PRV_SES`,`s`.`CODIGO_FAM_SES` AS `CODIGO_FAM_SES`,`s`.`ESTADO_SES` AS `ESTADO_SES`,(select count(0) from `fza_compras_sesiones_lineas` `l` where `l`.`SERIE_SES_SESLIN` = `s`.`SERIE_SES` and `l`.`NUMERO_SES_SESLIN` = `s`.`NUMERO_SES`) AS `NUM_LINEAS`,(select count(0) from `fza_compras_sesiones_celdas` `c` where `c`.`SERIE_SES_SESCEL` = `s`.`SERIE_SES` and `c`.`NUMERO_SES_SESCEL` = `s`.`NUMERO_SES` and `c`.`CANTIDAD_SESCEL` > 0) AS `NUM_SKUS_POTENCIALES`,(select ifnull(sum(`l`.`TOTAL_LINEA_SESLIN`),0) from `fza_compras_sesiones_lineas` `l` where `l`.`SERIE_SES_SESLIN` = `s`.`SERIE_SES` and `l`.`NUMERO_SES_SESLIN` = `s`.`NUMERO_SES`) AS `TOTAL_COMPRA` from `fza_compras_sesiones` `s`;

-- Recreando vista: vi_ses_resumen_almacen
DROP VIEW IF EXISTS `vi_ses_resumen_almacen`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_ses_resumen_almacen` AS select `c`.`SERIE_SES_SESCEL` AS `SERIE`,`c`.`NUMERO_SES_SESCEL` AS `NUMERO`,if(`c`.`CODIGO_ALM_SESCEL` = '',`s`.`CODIGO_ALM_SES`,`c`.`CODIGO_ALM_SESCEL`) AS `CODIGO_ALM`,count(0) AS `NUM_SKUS`,sum(`c`.`CANTIDAD_SESCEL`) AS `UNIDADES_TOTAL` from (`fza_compras_sesiones_celdas` `c` join `fza_compras_sesiones` `s` on(`s`.`SERIE_SES` = `c`.`SERIE_SES_SESCEL` and `s`.`NUMERO_SES` = `c`.`NUMERO_SES_SESCEL`)) where `c`.`CANTIDAD_SESCEL` > 0 group by `c`.`SERIE_SES_SESCEL`,`c`.`NUMERO_SES_SESCEL`,if(`c`.`CODIGO_ALM_SESCEL` = '',`s`.`CODIGO_ALM_SES`,`c`.`CODIGO_ALM_SESCEL`);

-- Recreando vista: vi_tarifas
DROP VIEW IF EXISTS `vi_tarifas`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_tarifas` AS select `fza_tarifas`.`CODIGO_TAR_ARTTAR` AS `CODIGO_TAR_ARTTAR`,`fza_tarifas`.`NOMBRE_TAR_TAR` AS `NOMBRE_TAR_TAR`,`fza_tarifas`.`ESACTIVO_ARTTAR` AS `ESACTIVO_ARTTAR`,`fza_tarifas`.`ORDEN_TAR` AS `ORDEN_TAR`,`fza_tarifas`.`ESIMP_INCL_TAR` AS `ESIMP_INCL_TAR`,`fza_tarifas`.`PORCENTAJE_MARGEN_TAR` AS `PORCENTAJE_MARGEN_TAR`,`fza_tarifas`.`VALOR_MULTIPLO_AJUSTE_TAR` AS `VALOR_MULTIPLO_AJUSTE_TAR`,`fza_tarifas`.`VALOR_MENOS_AJUSTE_TAR` AS `VALOR_MENOS_AJUSTE_TAR`,`fza_tarifas`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_tarifas`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_tarifas`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_tarifas`.`USUARIO_MODIF` AS `USUARIO_MODIF` from `fza_tarifas` where `fza_tarifas`.`ESACTIVO_ARTTAR` = 'S' order by `fza_tarifas`.`ORDEN_TAR`;

-- Recreando vista: vi_usuarios
DROP VIEW IF EXISTS `vi_usuarios`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_usuarios` AS select `fza_usuarios`.`USUARIO_USU` AS `USUARIO_USU`,`fza_usuarios`.`PASSWORD_USU` AS `PASSWORD_USU`,`fza_usuarios`.`GRUPO_USU` AS `GRUPO_USU`,`fza_usuarios`.`ESACTIVO_USU` AS `ESACTIVO_USU`,`fza_usuarios`.`EMPRESA_DEFECTO_USU` AS `EMPRESA_DEFECTO_USU`,`fza_usuarios`.`ULTIMO_LOGIN_USU` AS `ULTIMO_LOGIN_USU`,`fza_usuarios`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_usuarios`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_usuarios`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_usuarios`.`USUARIO_MODIF` AS `USUARIO_MODIF`,`fza_usuarios`.`ALMACEN_DEFECTO_USU` AS `ALMACEN_DEFECTO_USU`,`fza_usuarios`.`CAJA_DEFECTO_USU` AS `CAJA_DEFECTO_USU`,`vi_empresas`.`RAZON_SOCIAL_EMP` AS `RAZON_SOCIAL_EMP`,`fza_usuarios_grupos`.`GRUPO_USUGRP` AS `GRUPO_USUGRP`,`fza_usuarios_grupos`.`ESGRUPOADMINISTRADOR_USUGRP` AS `ESGRUPOADMINISTRADOR_USUGRP` from ((`fza_usuarios` join `fza_usuarios_grupos` on(`fza_usuarios`.`GRUPO_USU` = `fza_usuarios_grupos`.`GRUPO_USUGRP`)) left join `vi_empresas` on(`fza_usuarios`.`EMPRESA_DEFECTO_USU` = `vi_empresas`.`CODIGO_EMP_EMP`));

-- Recreando vista: vi_usuarios_grupos
DROP VIEW IF EXISTS `vi_usuarios_grupos`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_usuarios_grupos` AS select `fza_usuarios_grupos`.`GRUPO_USUGRP` AS `GRUPO_USUGRP`,`fza_usuarios_grupos`.`ESGRUPOADMINISTRADOR_USUGRP` AS `ESGRUPOADMINISTRADOR_USUGRP`,`fza_usuarios_grupos`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_usuarios_grupos`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_usuarios_grupos`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_usuarios_grupos`.`USUARIO_MODIF` AS `USUARIO_MODIF` from `fza_usuarios_grupos`;

-- Recreando vista: vi_usuarios_perfiles
DROP VIEW IF EXISTS `vi_usuarios_perfiles`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_usuarios_perfiles` AS select `fza_usuarios_perfiles`.`USUARIO_GRUPO_USUPER` AS `USUARIO_GRUPO_USUPER`,`fza_usuarios_perfiles`.`KEY_USUPER` AS `KEY_USUPER`,`fza_usuarios_perfiles`.`SUBKEY_USUPER` AS `SUBKEY_USUPER`,`fza_usuarios_perfiles`.`VALUE_USUPER` AS `VALUE_USUPER`,`fza_usuarios_perfiles`.`VALUE_TEXT_USUPER` AS `VALUE_TEXT_USUPER`,`fza_usuarios_perfiles`.`TYPE_BLOB_USUPER` AS `TYPE_BLOB_USUPER`,`fza_usuarios_perfiles`.`VALUE_BLOB_USUPER` AS `VALUE_BLOB_USUPER`,`fza_usuarios_perfiles`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_usuarios_perfiles`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_usuarios_perfiles`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_usuarios_perfiles`.`USUARIO_MODIF` AS `USUARIO_MODIF` from `fza_usuarios_perfiles`;

-- Recreando vista: vi_variaciones
DROP VIEW IF EXISTS `vi_variaciones`;

CREATE ALGORITHM=UNDEFINED  VIEW `vi_variaciones` AS select `fza_variaciones`.`CODIGO_VAR` AS `CODIGO_VAR`,`fza_variaciones`.`NOMBRE_VAR` AS `NOMBRE_VAR`,`fza_variaciones`.`ESACTIVO_VAR` AS `ESACTIVO_VAR`,`fza_variaciones`.`ORDEN_VAR` AS `ORDEN_VAR`,`fza_variaciones`.`INSTANTE_MODIF` AS `INSTANTE_MODIF`,`fza_variaciones`.`INSTANTE_ALTA` AS `INSTANTE_ALTA`,`fza_variaciones`.`USUARIO_ALTA` AS `USUARIO_ALTA`,`fza_variaciones`.`USUARIO_MODIF` AS `USUARIO_MODIF` from `fza_variaciones`;

-- Recreando vista: v_articulos_stock_barras
DROP VIEW IF EXISTS `v_articulos_stock_barras`;

CREATE ALGORITHM=UNDEFINED  VIEW `v_articulos_stock_barras` AS select coalesce(`s`.`CODIGO_UNIDAD_SKU`,`a`.`CODIGO_ART_ART`) AS `SKU`,`a`.`DESCRIPCION_ART` AS `DESCRIPCION_ART`,`cb`.`CODIGO_BARRAS_CB` AS `CODIGO_BARRAS`,coalesce(sum(`stk`.`CANTIDAD_STK`),0) AS `STOCK_TOTAL` from (((`fza_articulos` `a` left join `fza_articulos_skus` `s` on(`a`.`CODIGO_ART_ART` = `s`.`CODIGO_ART_SKU`)) left join `fza_codigos_barras` `cb` on(`cb`.`CODIGO_UNIDAD_CB` = coalesce(`s`.`CODIGO_UNIDAD_SKU`,`a`.`CODIGO_ART_ART`))) left join `fza_articulos_stockactual` `stk` on(`stk`.`CODIGO_UNIDAD_STK` = coalesce(`s`.`CODIGO_UNIDAD_SKU`,`a`.`CODIGO_ART_ART`))) group by coalesce(`s`.`CODIGO_UNIDAD_SKU`,`a`.`CODIGO_ART_ART`),`a`.`DESCRIPCION_ART`,`cb`.`CODIGO_BARRAS_CB`;

-- === PROCEDIMIENTOS ===
-- Recreando procedimiento: PRC_ADD_INDEX_IF_NOT_EXISTS
DROP PROCEDURE IF EXISTS `PRC_ADD_INDEX_IF_NOT_EXISTS`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_ADD_INDEX_IF_NOT_EXISTS`(IN `p_tabla` VARCHAR(64),    /* Nombre de la tabla destino, sin backticks */
    IN `p_indice` VARCHAR(64),       /* Nombre del indice a crear (convencion IDX_<SUF>_<col>) */
    IN `p_columnas` VARCHAR(1000))
BEGIN
    /* Crea un indice si no existe ya en la BBDD actual. Idempotente y
       seguro de re-ejecutar. Pensado para migraciones de rendimiento. */
    IF NOT EXISTS (
        SELECT 1
          FROM information_schema.statistics
         WHERE table_schema = DATABASE()
           AND table_name   = p_tabla
           AND index_name   = p_indice
    ) THEN
        SET @ddl = CONCAT('ALTER TABLE `', p_tabla,
                          '` ADD INDEX `', p_indice,
                          '` (', p_columnas, ')');
        PREPARE stmt FROM @ddl;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_AGREGAR_VALOR_CONJUNTO
DROP PROCEDURE IF EXISTS `PRC_AGREGAR_VALOR_CONJUNTO`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_AGREGAR_VALOR_CONJUNTO`(IN `p_id_conjunto` INT,         /* ID de la Paleta (ej: 5 para 'Verano 2026') */
    IN `p_valor_texto` VARCHAR(100), /* Lo que escribió el usuario (ej: 'VERDE RADIOACTIVO') */
    IN `p_usuario` VARCHAR(100))
BEGIN
    DECLARE v_id_valor INT;
    DECLARE v_tipo_variacion VARCHAR(20);
    
    /* 1. Averiguamos de qué tipo es esta paleta (¿Es de Colores CO o Tallas TC?) */
    SELECT ID_VA_AC INTO v_tipo_variacion
    FROM fza_atributos_conjuntos
    WHERE ID_AC = p_id_conjunto;
    
    /* 2. Usamos el truco del "Find or Create" que vimos antes */
    /* Esto busca el ID de 'VERDE RADIOACTIVO'. Si no existe, lo crea en fza_atributos_valores */
    CALL PRC_GET_CREAR_VALOR(v_tipo_variacion, p_valor_texto, p_usuario, v_id_valor);
    
    /* 3. Ahora que SEGURO tenemos un ID (v_id_valor), lo vinculamos a la paleta */
    /* Usamos INSERT IGNORE para que si ya estaba en la paleta, no de error */
    INSERT IGNORE INTO fza_atributos_conjuntos_det (
        ID_AC_ACD, 
        ID_AV_ACD, 
        USUARIO_ALTA, 
        USUARIO_MODIF, 
        INSTANTE_ALTA
    ) VALUES (
        p_id_conjunto, 
        v_id_valor, 
        p_usuario, 
        p_usuario, 
        NOW()
    );
    
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_ALB_CREAR_FACTURA_FIN
DROP PROCEDURE IF EXISTS `PRC_ALB_CREAR_FACTURA_FIN`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_ALB_CREAR_FACTURA_FIN`(  IN p_NUMERO_FAC varchar(20),  IN p_SERIE_FAC  varchar(20),  IN p_NUMERO_ALB varchar(20),  IN p_SERIE_ALB  varchar(20),  IN p_USUARIO    varchar(100))
BEGIN   DECLARE v_total_base decimal(18,6) DEFAULT 0;   DECLARE v_total_iva  decimal(18,6) DEFAULT 0;   DECLARE v_pendientes int DEFAULT 0;   SELECT IFNULL(SUM(CANTIDAD_FACLIN * PRECIO_VENTA_SIVA_ARTICULO_FACLIN), 0),          IFNULL(SUM(CANTIDAD_FACLIN * (PRECIO_VENTA_CIVA_ARTICULO_FACLIN - PRECIO_VENTA_SIVA_ARTICULO_FACLIN)), 0)     INTO v_total_base, v_total_iva     FROM fza_facturas_lineas    WHERE NUMERO_FAC_FACLIN = p_NUMERO_FAC      AND SERIE_FAC_FACLIN  = p_SERIE_FAC;   UPDATE fza_facturas      SET TOTAL_BASES_FAC     = v_total_base,          TOTAL_IMPUESTOS_FAC = v_total_iva,          TOTAL_LIQUIDO_FAC   = v_total_base + v_total_iva,          INSTANTE_MODIF      = NOW(),          USUARIO_MODIF       = p_USUARIO    WHERE NUMERO_FAC = p_NUMERO_FAC AND SERIE_FAC = p_SERIE_FAC;   IF p_NUMERO_ALB IS NOT NULL AND p_NUMERO_ALB <> '' THEN     SELECT COUNT(*) INTO v_pendientes       FROM fza_albaranes_lineas      WHERE NUMERO_ALB_ALBLIN = p_NUMERO_ALB        AND SERIE_ALB_ALBLIN  = p_SERIE_ALB        AND IFNULL(ESFACTURADA_ALBLIN, 'N') <> 'S';     IF v_pendientes = 0 THEN       UPDATE fza_albaranes          SET ESTADO_ALB    = 'FACTURADO',              NUMERO_FAC_ALB = p_NUMERO_FAC,              SERIE_FAC_ALB  = p_SERIE_FAC,              INSTANTE_MODIF = NOW(),              USUARIO_MODIF  = p_USUARIO        WHERE NUMERO_ALB = p_NUMERO_ALB AND SERIE_ALB = p_SERIE_ALB;     ELSE       UPDATE fza_albaranes          SET ESTADO_ALB    = 'PARCIAL',              INSTANTE_MODIF= NOW(),              USUARIO_MODIF = p_USUARIO        WHERE NUMERO_ALB = p_NUMERO_ALB AND SERIE_ALB = p_SERIE_ALB;     END IF;   END IF; END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_ALB_CREAR_FACTURA_INICIO
DROP PROCEDURE IF EXISTS `PRC_ALB_CREAR_FACTURA_INICIO`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_ALB_CREAR_FACTURA_INICIO`(  IN  p_NUMERO_ALB varchar(20),  IN  p_SERIE_ALB  varchar(20),  IN  p_USUARIO    varchar(100),  OUT p_NUMERO_FAC varchar(20),  OUT p_SERIE_FAC  varchar(20))
BEGIN   DECLARE v_serie  varchar(20);   DECLARE v_numero varchar(20);   SELECT SERIE_ALB INTO v_serie FROM fza_albaranes    WHERE NUMERO_ALB = p_NUMERO_ALB AND SERIE_ALB = p_SERIE_ALB;   SELECT LPAD(IFNULL(MAX(CAST(NUMERO_FAC AS UNSIGNED)), 0) + 1, 6, '0')     INTO v_numero FROM fza_facturas WHERE SERIE_FAC = v_serie;   INSERT INTO fza_facturas (     NUMERO_FAC, SERIE_FAC, FECHA_FAC, FASE_FAC, TIPO_FAC,     CODIGO_EMP_FAC, RAZON_SOCIAL_EMPRESA_FAC, NIF_EMPRESA_FAC,     MOVIL_EMPRESA_FAC, EMAIL_EMPRESA_FAC,     DIRECCION1_EMPRESA_FAC, DIRECCION2_EMPRESA_FAC,     POBLACION_EMPRESA_FAC, PROVINCIA_EMPRESA_FAC,     CODIGO_PAI_EMPRESA_FAC, NOMBRE_PAI_EMPRESA_FAC,     CODIGO_POSTAL_EMPRESA_FAC, GRUPO_ZONA_IVA_EMPRESA_FAC,     CODIGO_CLI_FAC, RAZON_SOCIAL_CLIENTE_FAC, NIF_CLIENTE_FAC,     MOVIL_CLIENTE_FAC, EMAIL_CLIENTE_FAC,     DIRECCION1_CLIENTE_FAC, DIRECCION2_CLIENTE_FAC,     POBLACION_CLIENTE_FAC, PROVINCIA_CLIENTE_FAC,     CODIGO_POSTAL_CLIENTE_FAC,     CODIGO_PAI_CLIENTE_FAC, NOMBRE_PAI_CLIENTE_FAC,     CODIGO_IVA_FAC,     ESIVA_RECARGO_CLIENTE_FAC, ESIVA_EXENTO_CLIENTE_FAC,     ESINTRACOMUNITARIO_CLIENTE_FAC,     TARIFA_ARTICULO_CLIENTE_FAC, ESIMP_INCL_TARIFA_CLIENTE_FAC,     PORCENTAJE_IVAN_FAC, PORCENTAJE_IVAR_FAC,     PORCENTAJE_IVAS_FAC, PORCENTAJE_IVAE_FAC,     FORMA_PAGO_FAC, CONTADOR_LINEAS_FAC,     INSTANTE_ALTA, USUARIO_ALTA, USUARIO_MODIF)   SELECT v_numero, v_serie, CURRENT_DATE(), 'BORRADOR', 'NORMAL',          A.CODIGO_EMP_ALB, A.RAZON_SOCIAL_EMPRESA_ALB, A.NIF_EMPRESA_ALB,          A.MOVIL_EMPRESA_ALB, A.EMAIL_EMPRESA_ALB,          A.DIRECCION1_EMPRESA_ALB, A.DIRECCION2_EMPRESA_ALB,          A.POBLACION_EMPRESA_ALB, A.PROVINCIA_EMPRESA_ALB,          A.CODIGO_PAI_EMPRESA_ALB, A.NOMBRE_PAI_EMPRESA_ALB,          A.CODIGO_POSTAL_EMPRESA_ALB, A.GRUPO_ZONA_IVA_EMPRESA_ALB,          A.CODIGO_CLI_ALB, A.RAZON_SOCIAL_CLIENTE_ALB, A.NIF_CLIENTE_ALB,          A.MOVIL_CLIENTE_ALB, A.EMAIL_CLIENTE_ALB,          A.DIRECCION1_CLIENTE_ALB, A.DIRECCION2_CLIENTE_ALB,          A.POBLACION_CLIENTE_ALB, A.PROVINCIA_CLIENTE_ALB,          A.CODIGO_POSTAL_CLIENTE_ALB,          A.CODIGO_PAI_CLIENTE_ALB, A.NOMBRE_PAI_CLIENTE_ALB,          A.CODIGO_IVA_ALB,          A.ESIVA_RECARGO_CLIENTE_ALB, A.ESIVA_EXENTO_CLIENTE_ALB,          A.ESINTRACOMUNITARIO_CLIENTE_ALB,          A.TARIFA_ARTICULO_CLIENTE_ALB, A.ESIMP_INCL_TARIFA_CLIENTE_ALB,          A.PORCENTAJE_IVAN_ALB, A.PORCENTAJE_IVAR_ALB,          A.PORCENTAJE_IVAS_ALB, A.PORCENTAJE_IVAE_ALB,          A.FORMA_PAGO_ALB, '0', NOW(), p_USUARIO, p_USUARIO     FROM fza_albaranes A    WHERE A.NUMERO_ALB = p_NUMERO_ALB AND A.SERIE_ALB = p_SERIE_ALB;   SET p_NUMERO_FAC = v_numero;   SET p_SERIE_FAC  = v_serie; END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_ALB_CREAR_FACTURA_LINEA
DROP PROCEDURE IF EXISTS `PRC_ALB_CREAR_FACTURA_LINEA`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_ALB_CREAR_FACTURA_LINEA`(  IN  p_NUMERO_FAC varchar(20),  IN  p_SERIE_FAC  varchar(20),  IN  p_NUMERO_ALB varchar(20),  IN  p_SERIE_ALB  varchar(20),  IN  p_LINEA_ALB  varchar(4),  IN  p_USUARIO    varchar(100))
PRC: BEGIN   DECLARE v_linea_fac varchar(4);   DECLARE v_facturada varchar(1);   SELECT IFNULL(ESFACTURADA_ALBLIN, 'N') INTO v_facturada     FROM fza_albaranes_lineas    WHERE NUMERO_ALB_ALBLIN = p_NUMERO_ALB      AND SERIE_ALB_ALBLIN  = p_SERIE_ALB      AND LINEA_ALBLIN      = p_LINEA_ALB;   IF v_facturada = 'S' THEN     LEAVE PRC;   END IF;   SELECT LPAD(IFNULL(MAX(CAST(LINEA_FACLIN AS UNSIGNED)), 0) + 10, 4, '0')     INTO v_linea_fac FROM fza_facturas_lineas    WHERE NUMERO_FAC_FACLIN = p_NUMERO_FAC      AND SERIE_FAC_FACLIN  = p_SERIE_FAC;   INSERT INTO fza_facturas_lineas (     NUMERO_FAC_FACLIN, SERIE_FAC_FACLIN, LINEA_FACLIN,     CODIGO_ART_FACLIN, CODIGO_UNIDAD_FACLIN,     LOTE_FACLIN, FECHA_CADUCIDAD_FACLIN,     CODIGO_FAM_FACLIN, NOMBRE_FAM_FACLIN,     DESCRIPCION_ARTICULO_FACLIN, DESCRIPCION_VARIACION_FACLIN,     TIPO_CANTIDAD_ARTICULO_FACLIN,     CANTIDAD_FACLIN, CODIGO_TAR_FACLIN, ESIMP_INCL_TARIFA_FACLIN,     TIPO_IVA_ARTICULO_FACLIN, PORCENTAJE_IVA_FACLIN,     PRECIO_VENTA_SIVA_ARTICULO_FACLIN,     PRECIO_VENTA_CIVA_ARTICULO_FACLIN,     TOTAL_FACLIN, CODIGO_ALM_FACLIN,     INSTANTE_ALTA, USUARIO_ALTA, USUARIO_MODIF)   SELECT p_NUMERO_FAC, p_SERIE_FAC, v_linea_fac,          AL.CODIGO_ART_ALBLIN, AL.CODIGO_UNIDAD_ALBLIN,          AL.LOTE_ALBLIN, AL.FECHA_CADUCIDAD_ALBLIN,          AL.CODIGO_FAM_ALBLIN, AL.NOMBRE_FAM_ALBLIN,          AL.DESCRIPCION_ARTICULO_ALBLIN, AL.DESCRIPCION_VARIACION_ALBLIN,          AL.TIPO_CANTIDAD_ARTICULO_ALBLIN,          AL.CANTIDAD_ALBLIN, AL.CODIGO_TAR_ALBLIN, AL.ESIMP_INCL_TARIFA_ALBLIN,          AL.TIPO_IVA_ARTICULO_ALBLIN, AL.PORCENTAJE_IVA_ALBLIN,          AL.PRECIO_VENTA_SIVA_ARTICULO_ALBLIN,          AL.PRECIO_VENTA_CIVA_ARTICULO_ALBLIN,          AL.TOTAL_ALBLIN, AL.CODIGO_ALMACEN_ALBLIN,          NOW(), p_USUARIO, p_USUARIO     FROM fza_albaranes_lineas AL    WHERE AL.NUMERO_ALB_ALBLIN = p_NUMERO_ALB      AND AL.SERIE_ALB_ALBLIN  = p_SERIE_ALB      AND AL.LINEA_ALBLIN      = p_LINEA_ALB;   UPDATE fza_albaranes_lineas      SET ESFACTURADA_ALBLIN = 'S',          NUMERO_FAC_ALBLIN  = p_NUMERO_FAC,          SERIE_FAC_ALBLIN   = p_SERIE_FAC,          LINEA_FAC_ALBLIN   = v_linea_fac,          INSTANTE_MODIF     = NOW(),          USUARIO_MODIF      = p_USUARIO    WHERE NUMERO_ALB_ALBLIN = p_NUMERO_ALB      AND SERIE_ALB_ALBLIN  = p_SERIE_ALB      AND LINEA_ALBLIN      = p_LINEA_ALB; END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_BUSQUEDA_ARTICULOS
DROP PROCEDURE IF EXISTS `PRC_BUSQUEDA_ARTICULOS`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_BUSQUEDA_ARTICULOS`(IN p_tarifa     VARCHAR(10),
    IN p_almacen    VARCHAR(10),
    IN p_fecha      DATE,
    IN p_token      VARCHAR(100),
    IN p_solostock  TINYINT,
    IN p_solotarifa TINYINT)
BEGIN
    IF p_tarifa IS NULL OR TRIM(p_tarifa) = '' THEN
        SET p_tarifa := 'PVP';
    END IF;
    IF p_fecha IS NULL THEN
        SET p_fecha := CURDATE();
    END IF;

    SELECT
        v.*,
        COALESCE(stk.STOCK_DISPONIBLE, 0) AS STOCK_DISPONIBLE
    FROM vi_art_busquedas v

    /* Stock unificado: por SKU + por código directo */
    LEFT JOIN (
        SELECT COD_ART, SUM(STOCK) AS STOCK_DISPONIBLE
        FROM (
            /* Artículos CON SKU: sumar por artículo padre */
            SELECT sku.CODIGO_ART_SKU AS COD_ART,
                   s.CANTIDAD_STK          AS STOCK
              FROM fza_articulos_stockactual s
              JOIN fza_articulos_skus sku
                ON s.CODIGO_UNIDAD_STK = sku.CODIGO_UNIDAD_SKU
             WHERE s.CODIGO_ALM_STK = p_almacen
            UNION ALL
            /* Artículos SIN SKU: código directo no existe en fza_articulos_skus */
            SELECT s.CODIGO_UNIDAD_STK AS COD_ART,
                   s.CANTIDAD_STK      AS STOCK
              FROM fza_articulos_stockactual s
             WHERE s.CODIGO_ALM_STK = p_almacen
               AND NOT EXISTS (
                   SELECT 1 FROM fza_articulos_skus sku2
                    WHERE sku2.CODIGO_UNIDAD_SKU = s.CODIGO_UNIDAD_STK
               )
        ) t
        GROUP BY COD_ART
    ) stk ON v.CODIGO_ART_ART = stk.COD_ART

    WHERE (v.CODIGO_TAR_ARTTAR = p_tarifa OR v.CODIGO_TAR_ARTTAR IS NULL)
      AND v.FECHA_DESDE_ARTTAR <= p_fecha
      AND (v.FECHA_HASTA_ARTTAR IS NULL OR v.FECHA_HASTA_ARTTAR >= p_fecha)
      AND (p_token IS NULL OR p_token = ''
           OR v.CODIGO_ART_ART      LIKE p_token
           OR v.DESCRIPCION_ART LIKE p_token
           OR v.DESCRIPCION_FAM  LIKE p_token)
      AND (p_solostock  = 0 OR COALESCE(stk.STOCK_DISPONIBLE, 0) > 0)
      AND (p_solotarifa = 0 OR v.CODIGO_TAR_ARTTAR IS NOT NULL)

    ORDER BY v.CODIGO_ART_ART;

END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_CALCULAR_FACTURA_NETOS
DROP PROCEDURE IF EXISTS `PRC_CALCULAR_FACTURA_NETOS`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_CALCULAR_FACTURA_NETOS`(IN `pSERIE_FACTURA` VARCHAR(12), 
  IN `pNRO_FACTURA` VARCHAR(12))
BEGIN
  DECLARE pPRECIOSIVA decimal(19,6);
  DECLARE pPRECIOCIVA decimal(19,6);
  DECLARE pPORCEN decimal(19,6);
  DECLARE pTIPO VARCHAR(2);
  DECLARE pZONAIVA_RE varchar(1) DEFAULT '';          /* CAMBIADO */
  DECLARE pAPLICA_RE_CLIENTE varchar(1) DEFAULT '';   /* CAMBIADO */
  DECLARE pIVA_EXENTO varchar(1) DEFAULT '';
  DECLARE pREG_AG_EMP varchar(1) DEFAULT '';
  DECLARE pREG_AG_CLI varchar(1) DEFAULT '';
  DECLARE pINTRACOMUNITARIO varchar(1) DEFAULT '';
  DECLARE pAPLICA_RETENCIONES_CLI varchar(1) DEFAULT '';
  DECLARE pAPLICA_RETENCIONES_EMP varchar(1) DEFAULT '';
  DECLARE pPORCENREN decimal(19,6) DEFAULT 0;
  DECLARE pPORCENRER decimal(19,6) DEFAULT 0;
  DECLARE pPORCENRES decimal(19,6) DEFAULT 0;
  DECLARE pPORCENREE decimal(19,6) DEFAULT 0;
  DECLARE pSUMBASEN decimal(19,6) DEFAULT 0;
  DECLARE pSUMBASER decimal(19,6) DEFAULT 0;
  DECLARE pSUMBASES decimal(19,6) DEFAULT 0;
  DECLARE pSUMBASEE decimal(19,6) DEFAULT 0;
  DECLARE pTOTN decimal(19,6) DEFAULT 0;
  DECLARE pTOTR decimal(19,6) DEFAULT 0;
  DECLARE pTOTS decimal(19,6) DEFAULT 0;
  DECLARE pTOTE decimal(19,6) DEFAULT 0;
  DECLARE pTOTRECN decimal(19,6) DEFAULT 0;
  DECLARE pTOTRECR decimal(19,6) DEFAULT 0;
  DECLARE pTOTRECS decimal(19,6) DEFAULT 0;
  DECLARE pTOTRECE decimal(19,6) DEFAULT 0;
  DECLARE pSUMTOTREC decimal(19,6) DEFAULT 0;
  DECLARE pSUMTOT decimal(19,6) DEFAULT 0;
  DECLARE pPORCENN decimal(19,6) DEFAULT 0;
  DECLARE pTOTBASES decimal(19,6) DEFAULT 0;
  DECLARE pPORCENR decimal(19,6) DEFAULT 0;
  DECLARE pPORCENS decimal(19,6) DEFAULT 0;
  DECLARE pPORCENE decimal(19,6) DEFAULT 0;
  DECLARE pPORCENRET decimal(19,6) DEFAULT 0;
  DECLARE pGRUPO_ZONA_IVA varchar(12);
  DECLARE pCODIGO_IVA varchar(12);
  DECLARE pTOTALRET decimal(19,6) DEFAULT 0;
  DECLARE pFECHA datetime;
  DECLARE pLINEA varchar(3);
  DECLARE pIMP_INCL varchar(1);
  DECLARE pCANTIDAD decimal(19,6) DEFAULT 0;
  DECLARE pCODART varchar(20);
  DECLARE pTIPOIVAF varchar(1) DEFAULT 'X';
  DECLARE pIRPF_IMP_INCL VARCHAR(1);
  DECLARE pVENTA_ACT_FIJ VARCHAR(1);
  DECLARE finished INTEGER DEFAULT 0;
  DECLARE pCODEMPRESA varchar(8);
  
  DECLARE CUR_LINEAS 
          CURSOR FOR 
              SELECT LINEA_FACLIN,
                     CODIGO_ART_FACLIN,
                     PRECIO_VENTA_SIVA_ARTICULO_FACLIN AS PRECIOSIVA,
                     PRECIO_VENTA_CIVA_ARTICULO_FACLIN AS PRECIOCIVA,
                     PORCENTAJE_IVA_FACLIN AS PORCEN,
                     TIPO_IVA_ARTICULO_FACLIN AS TIPO,
                     ESIMP_INCL_TARIFA_FACLIN AS IMP_INCL,
                     CANTIDAD_FACLIN as CANTIDAD_ARTVIN
                FROM FZA_FACTURAS_LINEAS 
               WHERE SERIE_FAC_FACLIN = pSERIE_FACTURA 
                 AND NUMERO_FAC_FACLIN = pNRO_FACTURA;
                 
  DECLARE CONTINUE HANDLER 
          FOR NOT FOUND SET finished = 1;

    /* Manejo de errores para asegurar la consistencia */
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    kk: BEGIN
        ROLLBACK;
        RESIGNAL;
    END kk;
          
  START TRANSACTION;
  
  SELECT PORCENTAJE_IVAN_FAC,
         PORCENTAJE_IVAR_FAC,
         PORCENTAJE_IVAS_FAC,
         PORCENTAJE_IVAE_FAC,
         PORCENTAJE_RETENCION_FAC,
         ESAPLICA_RE_ZONA_IVA_FAC,
         ESIVA_RECARGO_CLIENTE_FAC,
         ESIVA_EXENTO_CLIENTE_FAC,
         ESRETENCIONES_CLIENTE_FAC,
         ESRETENCIONES_EMPRESA_FAC,
         ESIRPF_IMP_INCL_ZONA_IVA_FAC,
         PORCENTAJE_REN_FAC,
         PORCENTAJE_RER_FAC,
         PORCENTAJE_RES_FAC,
         PORCENTAJE_REE_FAC,
         ESREGIMENESPECIALAGRICOLA_EMPRESA_FAC,
         ESREGIMENESPECIALAGRICOLA_CLIENTE_FAC,
         GRUPO_ZONA_IVA_EMPRESA_FAC,
         CODIGO_IVA_FAC,
         ESINTRACOMUNITARIO_CLIENTE_FAC,
         FECHA_FAC,
         CODIGO_EMP_FAC,
         ESVENTA_ACTIVO_FIJO_FAC
    INTO pPORCENN,
         pPORCENR,
         pPORCENS,
         pPORCENE,
         pPORCENRET,
         pZONAIVA_RE,              /* CAMBIADO */
         pAPLICA_RE_CLIENTE,       /* CAMBIADO */
         pIVA_EXENTO,
         pAPLICA_RETENCIONES_CLI,
         pAPLICA_RETENCIONES_EMP,
         pIRPF_IMP_INCL,
         pPORCENREN,
         pPORCENRER,
         pPORCENRES,
         pPORCENREE,
         pREG_AG_EMP,
         pREG_AG_CLI,
         pGRUPO_ZONA_IVA,
         pCODIGO_IVA,
         pINTRACOMUNITARIO,
         pFECHA,
         pCODEMPRESA,
         pVENTA_ACT_FIJ
    FROM fza_facturas
   WHERE SERIE_FAC = pSERIE_FACTURA
     AND NUMERO_FAC = pNRO_FACTURA;
     
  /* Resto de la lógica igual hasta el cálculo de recargos... */
  
  /* CAMBIO EN LA CONDICIÓN DE RECARGOS: */
  IF ( (pZONAIVA_RE ='S') AND (pAPLICA_RE_CLIENTE = 'S') ) THEN
    SET pTOTRECN = pSUMBASEN * (1 + pPORCENREN/100) - pSUMBASEN;
    SET pTOTRECR = pSUMBASER * (1 + pPORCENRER/100) - pSUMBASER;
    SET pTOTRECS = pSUMBASES * (1 + pPORCENRES/100) - pSUMBASES;
    SET pTOTRECE = pSUMBASEE * (1 + pPORCENREE/100) - pSUMBASEE;
    SET pSUMTOTREC = pTOTRECN + pTOTRECR + pTOTRECS + pTOTRECE;
  ELSE 
    SET pTOTRECN = 0;
    SET pTOTRECR = 0;
    SET pTOTRECS = 0;
    SET pTOTRECE = 0;
    SET pSUMTOTREC = 0;
  END IF;
  
  /* ... resto de código ... */
  
  /* CAMBIO EN EL UPDATE FINAL: */
  UPDATE fza_facturas 
     SET TOTAL_BASEI_IVAN_FAC = pSUMBASEN,
         TOTAL_BASEI_IVAR_FAC = pSUMBASER,
         TOTAL_BASEI_IVAS_FAC = pSUMBASES,
         TOTAL_BASEI_IVAE_FAC = pSUMBASEE,
         TOTAL_IVAN_FAC = pTOTN,
         TOTAL_IVAR_FAC = pTOTR,
         TOTAL_IVAS_FAC = pTOTS,
         TOTAL_IVAE_FAC = pTOTE,
         TOTAL_REN_FAC = PTOTRECN,
         TOTAL_RER_FAC = PTOTRECR,
         TOTAL_RES_FAC = PTOTRECS,
         TOTAL_REE_FAC = PTOTRECE,
         TOTAL_BASES_FAC = pTOTBASES,
         TOTAL_RETENCION_FAC = pTOTALRET,
         TOTAL_LIQUIDO_FAC = pTOTBASES + pSUMTOT - PTOTALRET + pSUMTOTREC,
         ESIVA_EXENTO_CLIENTE_FAC = pIVA_EXENTO,
         ESRETENCIONES_CLIENTE_FAC = pAPLICA_RETENCIONES_CLI,
         ESRETENCIONES_EMPRESA_FAC = pAPLICA_RETENCIONES_EMP,
         /* NO ACTUALIZAR: ESIVA_RECARGO_CLIENTE_FAC */
         TOTAL_IMPUESTOS_FAC = pSUMTOTREC + pSUMTOT,
         GRUPO_ZONA_IVA_EMPRESA_FAC = pGRUPO_ZONA_IVA,
         CODIGO_IVA_FAC = pCODIGO_IVA,
         PORCENTAJE_IVAN_FAC = pPORCENN,
         PORCENTAJE_IVAE_FAC = pPORCENE,
         PORCENTAJE_IVAR_FAC = pPORCENR,
         PORCENTAJE_IVAS_FAC = pPORCENS,
         PORCENTAJE_RETENCION_FAC = pPORCENRET
   WHERE NUMERO_FAC = pNRO_FACTURA 
     AND SERIE_FAC = pSERIE_FACTURA;
     
  COMMIT;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_CREAR_ACTUALIZAR_ARTICULO
DROP PROCEDURE IF EXISTS `PRC_CREAR_ACTUALIZAR_ARTICULO`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_CREAR_ACTUALIZAR_ARTICULO`(
    IN pCODIGO_ARTICULO               varchar(20),
    IN pDESCRIPCION_ARTICULO          varchar(1000),
    IN pTIPOIVA_ARTICULO              varchar(2),
    IN pTIPO_CANTIDAD_ARTICULO        varchar(20),
    IN pESACTIVO_FIJO_ARTICULO        varchar(1),
    IN pCODIGO_FAMILIA                varchar(20),
    IN pNOMBRE_FAMILIA                varchar(200),
    IN pCODIGO_PROVEEDOR              varchar(20),
    IN pRAZONSOCIAL_PROVEEDOR         varchar(200),
    IN pESPROVEEDORPRINCIPAL          varchar(1),
    IN pPRECIO_ULT_COMPRA             decimal(19,6),
    IN pCODIGO_TARIFA                 varchar(20),
    IN pPRECIOSALIDA_TARIFA           decimal(19,6),
    IN pPRECIOFINAL_TARIFA            decimal(19,6),
    IN pPRECIO_DTO_TARIFA             decimal(19,6),
    IN pPORCEN_DTO_TARIFA             decimal(19,6),
    IN pUSUARIO                       varchar(100)
)
BEGIN
    DECLARE pCONT BIGINT;

    /* Manejo de errores para asegurar la consistencia */
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    kk: BEGIN
        ROLLBACK;
        RESIGNAL;
    END kk;
    
    START TRANSACTION;

    IF (TRIM(pCODIGO_ARTICULO) <> '') THEN
        
        /* Comprobar si el artículo existe */
        IF ( EXISTS( SELECT *
                     FROM fza_articulos
                     WHERE `CODIGO_ART_ART` = pCODIGO_ARTICULO) ) THEN
                     
            UPDATE fza_articulos
            SET 
                DESCRIPCION_ART      = pDESCRIPCION_ARTICULO,
                ESACTIVO_FIJO_ART    = pESACTIVO_FIJO_ARTICULO,
                TIPO_IVA_ART          = pTIPOIVA_ARTICULO,
                TIPO_CANTIDAD_ART    = pTIPO_CANTIDAD_ARTICULO,
                CODIGO_FAM_ART   = pCODIGO_FAMILIA,
                USUARIO_MODIF              = pUSUARIO,
                INSTANTE_MODIF             = CURRENT_TIMESTAMP 
            WHERE CODIGO_ART_ART = pCODIGO_ARTICULO;
            
        ELSE
            /* Si no existe, crear uno nuevo */
            CALL PRC_FNC_GET_NEXT_NRO_DOC('AO', pCONT);
            
            INSERT INTO fza_articulos (
                CODIGO_ART_ART,
                ORDEN_ART,        
                DESCRIPCION_ART,
                TIPO_IVA_ART,
                TIPO_CANTIDAD_ART,
                CODIGO_FAM_ART,
                ESACTIVO_FIJO_ART,
                USUARIO_MODIF,
                INSTANTE_MODIF,
                USUARIO_ALTA,
                INSTANTE_ALTA                      
            ) VALUES (
                pCODIGO_ARTICULO,                                                                                         
                pCONT,
                pDESCRIPCION_ARTICULO, 
                pTIPOIVA_ARTICULO,
                pTIPO_CANTIDAD_ARTICULO,
                pCODIGO_FAMILIA,
                pESACTIVO_FIJO_ARTICULO,
                pUSUARIO,
                CURRENT_TIMESTAMP,
                pUSUARIO,
                CURRENT_TIMESTAMP              
            );
            
        END IF;

        /* Llamadas adicionales que siempre ocurren si el código del artículo no estaba vacío */
        CALL PRC_CREAR_ACTUALIZAR_FAMILIA(pCODIGO_FAMILIA, pNOMBRE_FAMILIA, pUSUARIO);
        CALL PRC_CREAR_ACTUALIZAR_PROVEEDOR(pCODIGO_PROVEEDOR, pRAZONSOCIAL_PROVEEDOR, pUSUARIO);
        CALL PRC_CREAR_ACTUALIZAR_ARTICULO_PROVEEDOR(pCODIGO_ARTICULO, pCODIGO_PROVEEDOR, pESPROVEEDORPRINCIPAL, pPRECIO_ULT_COMPRA, pUSUARIO);
        CALL PRC_CREAR_ACTUALIZAR_TARIFA(pCODIGO_ARTICULO, pCODIGO_TARIFA, pPRECIOSALIDA_TARIFA, pPRECIOFINAL_TARIFA, pPRECIO_DTO_TARIFA, pPORCEN_DTO_TARIFA, pUSUARIO);
        
    END IF; 

    COMMIT;
    
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_CREAR_ACTUALIZAR_ARTICULO_PROVEEDOR
DROP PROCEDURE IF EXISTS `PRC_CREAR_ACTUALIZAR_ARTICULO_PROVEEDOR`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_CREAR_ACTUALIZAR_ARTICULO_PROVEEDOR`(
    IN pCODIGO_ARTICULO               varchar(20),
    IN pCODIGO_PROVEEDOR              varchar(20),
    IN pESPROVEEDORPRINCIPAL          varchar(1),
    IN pPRECIO_ULT_COMPRA             decimal(19,6),
    IN pUSUARIO                       varchar(100)
)
BEGIN

    /* Manejo de errores para asegurar la consistencia */
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    kk: BEGIN
        ROLLBACK;
        RESIGNAL;
    END kk;
    START TRANSACTION;

    IF ((TRIM(pCODIGO_PROVEEDOR) <> '') AND (TRIM(pCODIGO_ARTICULO) <> '')) THEN
        
        /* Comprobar si ya existe la relación artículo-proveedor */
        IF ( EXISTS( SELECT *
                     FROM fza_articulos_proveedores
                     WHERE `CODIGO_PRV_AP` = pCODIGO_PROVEEDOR
                       AND CODIGO_ART_AP = pCODIGO_ARTICULO )) THEN
                       
            UPDATE fza_articulos_proveedores
            SET 
                PRECIO_ULT_COMPRA_AP    = pPRECIO_ULT_COMPRA,
                ESPROVEEDORPRINCIPAL_AP = pESPROVEEDORPRINCIPAL,
                USUARIO_MODIF                            = pUSUARIO,
                INSTANTE_MODIF                           = CURRENT_TIMESTAMP             
            WHERE `CODIGO_PRV_AP` = pCODIGO_PROVEEDOR
              AND CODIGO_ART_AP = pCODIGO_ARTICULO;
              
        ELSE
            /* Si no existe, insertar */
            INSERT INTO fza_articulos_proveedores (
                CODIGO_PRV_AP, 
                CODIGO_ART_AP,                  
                PRECIO_ULT_COMPRA_AP,
                ESPROVEEDORPRINCIPAL_AP,                                                                                        
                USUARIO_MODIF,
                INSTANTE_MODIF,
                USUARIO_ALTA,
                INSTANTE_ALTA                     
            ) VALUES (
                pCODIGO_PROVEEDOR,
                pCODIGO_ARTICULO,
                pPRECIO_ULT_COMPRA,       
                pESPROVEEDORPRINCIPAL,                                                                                                      
                pUSUARIO,
                CURRENT_TIMESTAMP,
                pUSUARIO,
                CURRENT_TIMESTAMP              
            );
            
        END IF;
        
    END IF;        

    COMMIT;
    
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_CREAR_ACTUALIZAR_CLIENTE
DROP PROCEDURE IF EXISTS `PRC_CREAR_ACTUALIZAR_CLIENTE`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_CREAR_ACTUALIZAR_CLIENTE`(
    IN `pCODIGO_CLIENTE`                    varchar(20),
    IN `pRAZONSOCIAL_CLIENTE`               varchar(200),
    IN `pNIF_CLIENTE`                       varchar(50),
    IN `pMOVIL_CLIENTE`                     varchar(40),
    IN `pEMAIL_CLIENTE`                     varchar(200),
    IN `pDIRECCION1_CLIENTE`                varchar(200),
    IN `pDIRECCION2_CLIENTE`                varchar(200),
    IN `pPOBLACION_CLIENTE`                 varchar(200),
    IN `pPROVINCIA_CLIENTE`                 varchar(200),
    IN `pCPOSTAL_CLIENTE`                   varchar(15),
    IN `pCOD_PAIS_CLIENTE`                  varchar(3),
    IN `pPAIS_CLIENTE`                      varchar(150),
    IN `pESIVA_EXENTO_CLIENTE`              varchar(1),
    IN `pESRETENCIONES_CLIENTE`             varchar(1),
    IN `pESIVA_RECARGO_CLIENTE`             varchar(1),
    IN `pESINTRACOMUNITARIO_CLIENTE`        varchar(1),
    IN `pESREGIMENESPECIALAGRICOLA_CLIENTE` varchar(1),
    IN `pTARIFA_ARTICULO_CLIENTE`           varchar(20),
    IN `pUSUARIO`                           varchar(100)
)
BEGIN

    /* Manejo de errores para asegurar la consistencia */
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    kk: BEGIN
        ROLLBACK;
        RESIGNAL;
    END kk;
    START TRANSACTION;
    
    /* Comprobar si el cliente ya existe */
    IF ( EXISTS( SELECT *
                 FROM fza_clientes
                 WHERE `CODIGO_CLI_CLI` = pCODIGO_CLIENTE) ) THEN
                 
        UPDATE fza_clientes
        SET 
            RAZON_SOCIAL_CLI               = pRAZONSOCIAL_CLIENTE,
            NIF_CLI                       = pNIF_CLIENTE,
            MOVIL_CLI                     = pMOVIL_CLIENTE,
            EMAIL_CLI                     = pEMAIL_CLIENTE,
            DIRECCION1_CLI                = pDIRECCION1_CLIENTE,
            DIRECCION2_CLI                = pDIRECCION2_CLIENTE,
            POBLACION_CLI                 = pPOBLACION_CLIENTE,
            PROVINCIA_CLI                 = pPROVINCIA_CLIENTE,
            CODIGO_POSTAL_CLI                   = pCPOSTAL_CLIENTE,
            NOMBRE_PAI_CLI               = pPAIS_CLIENTE,
            CODIGO_PAI_CLI               = pCOD_PAIS_CLIENTE,
            ESIVA_EXENTO_CLI              = pESIVA_EXENTO_CLIENTE,
            ESRETENCIONES_CLI             = pESRETENCIONES_CLIENTE,
            ESIVA_RECARGO_CLI             = pESIVA_RECARGO_CLIENTE,
            ESREGIMENESPECIALAGRICOLA_CLI = pESREGIMENESPECIALAGRICOLA_CLIENTE,
            ESINTRACOMUNITARIO_CLI        = pESINTRACOMUNITARIO_CLIENTE,
            TARIFA_ARTICULO_CLI           = pTARIFA_ARTICULO_CLIENTE,
            USUARIO_MODIF                      = pUSUARIO,
            INSTANTE_MODIF                     = CURRENT_TIMESTAMP
        WHERE CODIGO_CLI_CLI = pCODIGO_CLIENTE;
        
    ELSE
    
        /* Si no existe, insertar */
        INSERT INTO fza_clientes (
            CODIGO_CLI_CLI,
            RAZON_SOCIAL_CLI,
            NIF_CLI,
            MOVIL_CLI,
            EMAIL_CLI,
            DIRECCION1_CLI,
            DIRECCION2_CLI,
            POBLACION_CLI,
            PROVINCIA_CLI,
            CODIGO_POSTAL_CLI,
            NOMBRE_PAI_CLI,
            CODIGO_PAI_CLI,
            ESIVA_EXENTO_CLI,
            ESRETENCIONES_CLI,
            ESIVA_RECARGO_CLI,
            ESREGIMENESPECIALAGRICOLA_CLI,
            ESINTRACOMUNITARIO_CLI,
            TARIFA_ARTICULO_CLI,
            USUARIO_MODIF,
            USUARIO_ALTA,
            INSTANTE_ALTA,
            INSTANTE_MODIF
        ) VALUES (
            pCODIGO_CLIENTE,
            pRAZONSOCIAL_CLIENTE,
            pNIF_CLIENTE,
            pMOVIL_CLIENTE,
            pEMAIL_CLIENTE,
            pDIRECCION1_CLIENTE,
            pDIRECCION2_CLIENTE,
            pPOBLACION_CLIENTE,
            pPROVINCIA_CLIENTE,
            pCPOSTAL_CLIENTE,
            pPAIS_CLIENTE,
            pCOD_PAIS_CLIENTE,
            pESIVA_EXENTO_CLIENTE,
            pESRETENCIONES_CLIENTE,
            pESIVA_RECARGO_CLIENTE,
            pESREGIMENESPECIALAGRICOLA_CLIENTE,
            pESINTRACOMUNITARIO_CLIENTE,
            pTARIFA_ARTICULO_CLIENTE,
            pUSUARIO,
            pUSUARIO,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        );
        
    END IF;
    
    COMMIT;
    
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_CREAR_ACTUALIZAR_EMPRESA
DROP PROCEDURE IF EXISTS `PRC_CREAR_ACTUALIZAR_EMPRESA`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_CREAR_ACTUALIZAR_EMPRESA`(
  in pCODIGO_EMPRESA varchar(10),
  in pRAZONSOCIAL_EMPRESA varchar(200),
  in pNIF_EMPRESA varchar(50),
  in pMOVIL_EMPRESA varchar(40),
  in pEMAIL_EMPRESA varchar(200),
  in pDIRECCION1_EMPRESA varchar(200),
  in pDIRECCION2_EMPRESA varchar(200),
  in pPOBLACION_EMPRESA varchar(200),
  in pPROVINCIA_EMPRESA varchar(200),
  in pCPOSTAL_EMPRESA varchar(15),
  in pPAIS_EMPRESA varchar(150),
  in pCODPAIS_EMPRESA varchar(150),
  in pRETENCIONES_EMPRESA varchar(1),
  in pIVA_RECARGO_EMPRESA varchar(1),
  in pREGIMENESPECIALAGRICOLA_EMPRESA varchar(1),
  in pGRUPO_ZONA_IVA_EMPRESA varchar(10),
  in pUSUARIO varchar(100))
BEGIN
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  kk: BEGIN
    ROLLBACK;
    RESIGNAL;
  END kk;
  START TRANSACTION;
  IF (EXISTS (
  SELECT *
  FROM fza_empresas
  WHERE CODIGO_EMP_EMP = pCODIGO_EMPRESA)) THEN
    UPDATE fza_EMPRESAs
      SET RAZON_SOCIAL_EMP = pRAZONSOCIAL_EMPRESA,
      NIF_EMP = pNIF_EMPRESA,
      MOVIL_EMP = pMOVIL_EMPRESA,
      EMAIL_EMP = pEMAIL_EMPRESA,
      DIRECCION1_EMP = pDIRECCION1_EMPRESA,
      DIRECCION2_EMP = pDIRECCION2_EMPRESA,
      POBLACION_EMP = pPOBLACION_EMPRESA,
      PROVINCIA_EMP = pPROVINCIA_EMPRESA,
      CODIGO_POSTAL_EMP = pCPOSTAL_EMPRESA,
      NOMBRE_PAI_EMP = pPAIS_EMPRESA,
      CODIGO_PAI_EMP = pCODPAIS_EMPRESA,
      ESRETENCIONES_EMP = pRETENCIONES_EMPRESA,
      ESREGIMENESPECIALAGRICOLA_EMP = pREGIMENESPECIALAGRICOLA_EMPRESA,
      GRUPO_ZONA_IVA_EMP = pGRUPO_ZONA_IVA_EMPRESA,
      USUARIO_MODIF = pUSUARIO,
      INSTANTE_MODIF = CURRENT_TIMESTAMP
      WHERE CODIGO_EMP_EMP = pCODIGO_EMPRESA;
  ELSE
    INSERT INTO fza_EMPRESAs(CODIGO_EMP_EMP,
      RAZON_SOCIAL_EMP,
      NIF_EMP,
      MOVIL_EMP,
      EMAIL_EMP,
      DIRECCION1_EMP,
      DIRECCION2_EMP,
      POBLACION_EMP,
      PROVINCIA_EMP,
      CODIGO_POSTAL_EMP,
      NOMBRE_PAI_EMP,
      CODIGO_PAI_EMP,
      ESRETENCIONES_EMP,
      ESREGIMENESPECIALAGRICOLA_EMP,
      GRUPO_ZONA_IVA_EMP,
      USUARIO_MODIF,
      USUARIO_ALTA,
      INSTANTE_ALTA,
      INSTANTE_MODIF
      )
      VALUES
      (pCODIGO_EMPRESA,
      pRAZONSOCIAL_EMPRESA,
      pNIF_EMPRESA,
      pMOVIL_EMPRESA,
      pEMAIL_EMPRESA,
      pDIRECCION1_EMPRESA,
      pDIRECCION2_EMPRESA,
      pPOBLACION_EMPRESA,
      pPROVINCIA_EMPRESA,
      pCPOSTAL_EMPRESA,
      pPAIS_EMPRESA,
      pCODPAIS_EMPRESA,
      pRETENCIONES_EMPRESA,
      pREGIMENESPECIALAGRICOLA_EMPRESA,
      pGRUPO_ZONA_IVA_EMPRESA,
      pUSUARIO,
      pUSUARIO,
      CURRENT_TIMESTAMP,
      CURRENT_TIMESTAMP
      );
  END IF;
  COMMIT;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_CREAR_ACTUALIZAR_FAMILIA
DROP PROCEDURE IF EXISTS `PRC_CREAR_ACTUALIZAR_FAMILIA`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_CREAR_ACTUALIZAR_FAMILIA`(
    IN pCODIGO_FAMILIA      varchar(20),
    IN pNOMBRE_FAMILIA      varchar(200),
    IN pUSUARIO             varchar(100)
)
BEGIN
    DECLARE pCONT BIGINT;

    /* Manejo de errores para asegurar la consistencia */
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    kk: BEGIN
        ROLLBACK;
        RESIGNAL;
    END kk;
    
    START TRANSACTION;

    IF (TRIM(pCODIGO_FAMILIA) <> '') THEN
        
        /* Comprobar si la familia ya existe */
        IF ( EXISTS( SELECT *
                     FROM fza_articulos_familias
                     WHERE `CODIGO_FAM_FAM` = pCODIGO_FAMILIA) ) THEN
                     
            UPDATE fza_articulos_familias
            SET 
                NOMBRE_FAM_FAM      = pNOMBRE_FAMILIA,
                DESCRIPCION_FAM = pNOMBRE_FAMILIA,
                USUARIO_MODIF        = pUSUARIO,
                INSTANTE_MODIF       = CURRENT_TIMESTAMP             
            WHERE CODIGO_FAM_FAM = pCODIGO_FAMILIA;
            
        ELSE
            /* Si no existe, insertar */
            CALL PRC_FNC_GET_NEXT_NRO_DOC('FO', pCONT);
            
            INSERT INTO fza_articulos_familias (
                CODIGO_FAM_FAM,
                ORDEN_FAM,
                NOMBRE_FAM_FAM,
                DESCRIPCION_FAM,
                USUARIO_MODIF,
                INSTANTE_MODIF,
                USUARIO_ALTA,
                INSTANTE_ALTA                     
            ) VALUES (
                pCODIGO_FAMILIA,
                pCONT,
                pNOMBRE_FAMILIA,
                pNOMBRE_FAMILIA,
                pUSUARIO,
                CURRENT_TIMESTAMP,
                pUSUARIO,
                CURRENT_TIMESTAMP              
            );
            
        END IF;
        
    END IF;        

    COMMIT;
    
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_CREAR_ACTUALIZAR_KEY
DROP PROCEDURE IF EXISTS `PRC_CREAR_ACTUALIZAR_KEY`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_CREAR_ACTUALIZAR_KEY`(
    IN pUSUARIO       varchar(200),
    IN pKEY           varchar(100),
    IN pSUBKEY        varchar(100),
    IN pVALUE         varchar(200),
    IN pVALUE_TEXT    text,
    IN pUSUARIO_MODIF varchar(100)
)
BEGIN

    /* Manejo de errores para asegurar la consistencia */
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    kk: BEGIN
        ROLLBACK;
        RESIGNAL;
    END kk;
    START TRANSACTION;    
    /* Comprobamos si el registro existe */
    IF ( EXISTS(
             SELECT 1
             FROM `fza_usuarios_perfiles`
             WHERE `USUARIO_GRUPO_USUPER` = pUSUARIO
               AND `KEY_USUPER`           = pKEY 
               AND `SUBKEY_USUPER`        = pSUBKEY 
        )) THEN
        
        UPDATE `fza_usuarios_perfiles`
        SET `VALUE_USUPER`      = pVALUE,
            `VALUE_TEXT_USUPER` = pVALUE_TEXT,
            `USUARIO_MODIF`        = pUSUARIO_MODIF
        WHERE `USUARIO_GRUPO_USUPER` = pUSUARIO
          AND `KEY_USUPER`           = pKEY 
          AND `SUBKEY_USUPER`        = pSUBKEY;
          
    ELSE
    
        INSERT INTO fza_usuarios_perfiles (
            `USUARIO_GRUPO_USUPER`, 
            `KEY_USUPER`, 
            `SUBKEY_USUPER`, 
            `VALUE_USUPER`, 
            `VALUE_TEXT_USUPER`, 
            `INSTANTE_ALTA`, 
            `USUARIO_ALTA`, 
            `USUARIO_MODIF`
        ) VALUES (
            pUSUARIO,
            pKEY,
            pSUBKEY,
            pVALUE,
            pVALUE_TEXT,
            CURRENT_TIMESTAMP,
            pUSUARIO_MODIF,
            pUSUARIO_MODIF
        );
        
    END IF;
    
    COMMIT;
    
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_CREAR_ACTUALIZAR_PROVEEDOR
DROP PROCEDURE IF EXISTS `PRC_CREAR_ACTUALIZAR_PROVEEDOR`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_CREAR_ACTUALIZAR_PROVEEDOR`(
    IN pCODIGO_PROVEEDOR      varchar(20),
    IN pRAZONSOCIAL_PROVEEDOR varchar(200),
    IN pUSUARIO               varchar(100)
)
BEGIN
    DECLARE pCONT BIGINT;

    /* Manejo de errores para asegurar la consistencia */
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    kk: BEGIN
        ROLLBACK;
        RESIGNAL;
    END kk;
    
    START TRANSACTION;

    IF (TRIM(pCODIGO_PROVEEDOR) <> '') THEN
        
        /* Comprobar si el proveedor ya existe */
        IF ( EXISTS( SELECT *
                     FROM fza_proveedores
                     WHERE `CODIGO_PRV_PRV` = pCODIGO_PROVEEDOR) ) THEN
                     
            UPDATE fza_proveedores
            SET 
                RAZON_SOCIAL_PRV = pRAZONSOCIAL_PROVEEDOR,
                USUARIO_MODIF          = pUSUARIO,
                INSTANTE_MODIF         = CURRENT_TIMESTAMP             
            WHERE CODIGO_PRV_PRV = pCODIGO_PROVEEDOR;
            
        ELSE
            /* Si no existe, obtener el siguiente número e insertar */
            CALL PRC_FNC_GET_NEXT_NRO_DOC('PO', pCONT);
            
            INSERT INTO fza_proveedores (
                CODIGO_PRV_PRV,
                ORDEN_PRV,
                RAZON_SOCIAL_PRV,
                USUARIO_MODIF,
                INSTANTE_MODIF,
                USUARIO_ALTA,
                INSTANTE_ALTA                     
            ) VALUES (
                pCODIGO_PROVEEDOR,
                pCONT,
                pRAZONSOCIAL_PROVEEDOR,
                pUSUARIO,
                CURRENT_TIMESTAMP,
                pUSUARIO,
                CURRENT_TIMESTAMP              
            );
            
        END IF;
        
    END IF;        

    COMMIT;
    
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_CREAR_ACTUALIZAR_TARIFA
DROP PROCEDURE IF EXISTS `PRC_CREAR_ACTUALIZAR_TARIFA`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_CREAR_ACTUALIZAR_TARIFA`(
    IN pCODIGO_ARTICULO     varchar(20),
    IN pCODIGO_TARIFA       varchar(20),
    IN pPRECIOSALIDA_TARIFA decimal(19,6),
    IN pPRECIOFINAL_TARIFA  decimal(19,6),
    IN pPRECIO_DTO_TARIFA   decimal(19,6),
    IN pPORCEN_DTO_TARIFA   decimal(19,6),
    IN pUSUARIO             varchar(100)
)
BEGIN

    DECLARE ppPRECIOSALIDA_TARIFA decimal(19,6);
    DECLARE ppPRECIOFINAL_TARIFA  decimal(19,6);
    DECLARE ppPORCEN_DTO_TARIFA   decimal(19,6);
    DECLARE ppPRECIO_DTO_TARIFA   decimal(19,6);

    /* Manejo de errores para asegurar la consistencia */
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    kk: BEGIN
        ROLLBACK;
        RESIGNAL;
    END kk;

    START TRANSACTION;

    IF (TRIM(pCODIGO_TARIFA) <> '') THEN
        
        /* Obtener precios actuales */
        CALL PRC_FNC_GET_PRECIO_ARTICULO_FECHA(
            pCODIGO_ARTICULO, 
            CURRENT_DATE, 
            ppPRECIOSALIDA_TARIFA, 
            ppPRECIOFINAL_TARIFA,
            ppPORCEN_DTO_TARIFA,
            ppPRECIO_DTO_TARIFA
        );

        IF (ppPRECIOFINAL_TARIFA IS NOT NULL) THEN
            
            UPDATE fza_articulos_tarifas
            SET 
                PRECIO_SALIDA_ARTTAR = pPRECIOSALIDA_TARIFA,
                PRECIO_FINAL_ARTTAR  = pPRECIOFINAL_TARIFA,
                PRECIO_DTO_ARTTAR   = pPRECIO_DTO_TARIFA,
                PORCENTAJE_DTO_ARTTAR   = pPORCEN_DTO_TARIFA,
                USUARIO_MODIF        = pUSUARIO,
                INSTANTE_MODIF       = CURRENT_TIMESTAMP             
            WHERE CODIGO_ART_ARTTAR = pCODIGO_ARTICULO
              AND CODIGO_TAR_ARTTAR = pCODIGO_TARIFA;
              
        ELSE
            
            INSERT INTO fza_articulos_tarifas ( 
                CODIGO_ART_ARTTAR,
                CODIGO_TAR_ARTTAR,
                PRECIO_SALIDA_ARTTAR,
                PRECIO_FINAL_ARTTAR,
                PRECIO_DTO_ARTTAR,
                PORCENTAJE_DTO_ARTTAR,
                FECHA_DESDE_ARTTAR,
                USUARIO_MODIF,
                INSTANTE_MODIF,
                USUARIO_ALTA,
                INSTANTE_ALTA                     
            ) VALUES (
                pCODIGO_ARTICULO,
                pCODIGO_TARIFA,
                pPRECIOSALIDA_TARIFA,
                pPRECIOFINAL_TARIFA,
                pPRECIO_DTO_TARIFA,
                pPORCEN_DTO_TARIFA,
                CURRENT_TIMESTAMP,
                pUSUARIO,
                CURRENT_TIMESTAMP,
                pUSUARIO,
                CURRENT_TIMESTAMP              
            );
            
        END IF;
        
    END IF;        
    
    COMMIT;
    
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_CREAR_ACTUALIZAR_TEST
DROP PROCEDURE IF EXISTS `PRC_CREAR_ACTUALIZAR_TEST`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_CREAR_ACTUALIZAR_TEST`()
BEGIN
  CALL PRC_FNC_GET_PRECIO_ARTICULO_FECHA('PAÑITOS', CURRENT_DATE, @PRECIOFINAL, @PRECIO_INICIAL, @PORCEN_DTO, @PRECIO_DTO);
  /* SELECT @PRECIOFINAL, @PRECIO_INICIAL, @PORCEN_DTO, @PRECIO_DTO; */
			IF (@PRECIOFINAL IS NULL) THEN
			  SELECT 'HOLA';
		 ELSE
		   SELECT 'NO HAY';
		 END IF;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_CREAR_FACTURA_ABONO
DROP PROCEDURE IF EXISTS `PRC_CREAR_FACTURA_ABONO`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_CREAR_FACTURA_ABONO`(
    IN `pidseriefactura`      varchar(200),
    IN `pidnumfactura`        varchar(200),
    IN `pidseriefacturaabono` varchar(200),
    IN `pidcodigo_empresa`    varchar(200),
    IN `pfechafacturaabono`   date,
    OUT `pidnumfacturaabono`  varchar(200),
    IN `pUSUARIO`             varchar(100)
)
BEGIN   
    DECLARE contadorped varchar(200);
    DECLARE pFecha date;

    /* Manejo de errores para asegurar la consistencia */    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    kk: BEGIN        
        ROLLBACK;
        RESIGNAL;
    END kk;

    START TRANSACTION;
    
    /* Pasamos contadorped directamente sin usar @cont */
    CALL PRC_GET_NEXT_CONT_FACT_SERIE(pidseriefacturaabono, 'FC', pidcodigo_empresa, pUSUARIO, contadorped);   
    
    /* Asignación limpia sin DATE_FORMAT innecesario */
    SET pFecha = pfechafacturaabono;
    SET pidnumfacturaabono = contadorped;

    INSERT INTO fza_facturas (
        `NUMERO_FAC`, `SERIE_FAC`, `FECHA_FAC`, `CODIGO_EMP_FAC`,
        `RAZON_SOCIAL_EMPRESA_FAC`, `NIF_EMPRESA_FAC`, `MOVIL_EMPRESA_FAC`, `EMAIL_EMPRESA_FAC`,
        `DIRECCION1_EMPRESA_FAC`, `DIRECCION2_EMPRESA_FAC`, `POBLACION_EMPRESA_FAC`, `PROVINCIA_EMPRESA_FAC`,
        `NOMBRE_PAI_EMPRESA_FAC`, `CODIGO_PAI_EMPRESA_FAC`, `CODIGO_POSTAL_EMPRESA_FAC`, `ESRETENCIONES_EMPRESA_FAC`,
        `GRUPO_ZONA_IVA_EMPRESA_FAC`, `ESREGIMENESPECIALAGRICOLA_EMPRESA_FAC`, `CODIGO_CLI_FAC`, `RAZON_SOCIAL_CLIENTE_FAC`,
        `NIF_CLIENTE_FAC`, `MOVIL_CLIENTE_FAC`, `EMAIL_CLIENTE_FAC`, `DIRECCION1_CLIENTE_FAC`,
        `DIRECCION2_CLIENTE_FAC`, `POBLACION_CLIENTE_FAC`, `PROVINCIA_CLIENTE_FAC`, `CODIGO_POSTAL_CLIENTE_FAC`,
        `NOMBRE_PAI_CLIENTE_FAC`, `CODIGO_PAI_CLIENTE_FAC`, `ESIVA_RECARGO_CLIENTE_FAC`, `ESIVA_EXENTO_CLIENTE_FAC`,
        `ESREGIMENESPECIALAGRICOLA_CLIENTE_FAC`, `ESRETENCIONES_CLIENTE_FAC`, `TARIFA_ARTICULO_CLIENTE_FAC`, `ESIMP_INCL_TARIFA_CLIENTE_FAC`,
        `ESINTRACOMUNITARIO_CLIENTE_FAC`, `ESIRPF_IMP_INCL_ZONA_IVA_FAC`, `ESAPLICA_RE_ZONA_IVA_FAC`, `ESIVAAGRICOLA_ZONA_IVA_FAC`,
        `PALABRA_REPORTS_ZONA_IVA_FAC`, `CODIGO_IVA_FAC`, `ESVENTA_ACTIVO_FIJO_FAC`, `PORCENTAJE_IVAN_FAC`,
        `TOTAL_IVAN_FAC`, `PORCENTAJE_REN_FAC`, `TOTAL_REN_FAC`, `TOTAL_BASEI_IVAN_FAC`,
        `PORCENTAJE_IVAR_FAC`, `TOTAL_IVAR_FAC`, `PORCENTAJE_RER_FAC`, `TOTAL_RER_FAC`,
        `TOTAL_BASEI_IVAR_FAC`, `PORCENTAJE_IVAS_FAC`, `TOTAL_IVAS_FAC`, `PORCENTAJE_RES_FAC`,
        `TOTAL_RES_FAC`, `TOTAL_BASEI_IVAS_FAC`, `PORCENTAJE_IVAE_FAC`, `TOTAL_IVAE_FAC`,
        `PORCENTAJE_REE_FAC`, `TOTAL_REE_FAC`, `TOTAL_BASEI_IVAE_FAC`, `TOTAL_BASES_FAC`,
        `TOTAL_IMPUESTOS_FAC`, `FORMA_PAGO_FAC`, `PORCENTAJE_RETENCION_FAC`, `TOTAL_RETENCION_FAC`,
        `TOTAL_LIQUIDO_FAC`, `NUMERO_FAC_ABONO_FAC`, `SERIE_FAC_ABONO_FAC`, `TEXTO_LEGAL_CLIENTE_FAC`,
        `TEXTO_LEGAL_EMPRESA_FAC`, `DOCUMENTO_FAC`, `COMENTARIOS_FAC`, `CONTADOR_LINEAS_FAC`,
        `ESCREARARTICULOS_FAC`, `ESDESCRIPCIONES_AMP_FAC`, `ESFECHADEENTREGA_FAC`, 
        `INSTANTE_MODIF`, `INSTANTE_ALTA`, `USUARIO_ALTA`, `USUARIO_MODIF`
    )                                 
    SELECT 
        contadorped,                 /* SIN comillas */
        pidseriefacturaabono,        /* SIN comillas */
        pFecha,                      /* SIN comillas (y sin la arroba) */
        `CODIGO_EMP_FAC`, `RAZON_SOCIAL_EMPRESA_FAC`, `NIF_EMPRESA_FAC`, `MOVIL_EMPRESA_FAC`, `EMAIL_EMPRESA_FAC`,
        `DIRECCION1_EMPRESA_FAC`, `DIRECCION2_EMPRESA_FAC`, `POBLACION_EMPRESA_FAC`, `PROVINCIA_EMPRESA_FAC`,
        `NOMBRE_PAI_EMPRESA_FAC`, `CODIGO_PAI_EMPRESA_FAC`, `CODIGO_POSTAL_EMPRESA_FAC`, `ESRETENCIONES_EMPRESA_FAC`,
        `GRUPO_ZONA_IVA_EMPRESA_FAC`, `ESREGIMENESPECIALAGRICOLA_EMPRESA_FAC`, `CODIGO_CLI_FAC`, `RAZON_SOCIAL_CLIENTE_FAC`,
        `NIF_CLIENTE_FAC`, `MOVIL_CLIENTE_FAC`, `EMAIL_CLIENTE_FAC`, `DIRECCION1_CLIENTE_FAC`,
        `DIRECCION2_CLIENTE_FAC`, `POBLACION_CLIENTE_FAC`, `PROVINCIA_CLIENTE_FAC`, `CODIGO_POSTAL_CLIENTE_FAC`,
        `NOMBRE_PAI_CLIENTE_FAC`, `CODIGO_PAI_CLIENTE_FAC`, `ESIVA_RECARGO_CLIENTE_FAC`, `ESIVA_EXENTO_CLIENTE_FAC`,
        `ESREGIMENESPECIALAGRICOLA_CLIENTE_FAC`, `ESRETENCIONES_CLIENTE_FAC`, `TARIFA_ARTICULO_CLIENTE_FAC`, `ESIMP_INCL_TARIFA_CLIENTE_FAC`,
        `ESINTRACOMUNITARIO_CLIENTE_FAC`, `ESIRPF_IMP_INCL_ZONA_IVA_FAC`, `ESAPLICA_RE_ZONA_IVA_FAC`, `ESIVAAGRICOLA_ZONA_IVA_FAC`,
        `PALABRA_REPORTS_ZONA_IVA_FAC`, `CODIGO_IVA_FAC`, `ESVENTA_ACTIVO_FIJO_FAC`, `PORCENTAJE_IVAN_FAC`,
        `TOTAL_IVAN_FAC`, `PORCENTAJE_REN_FAC`, `TOTAL_REN_FAC`, `TOTAL_BASEI_IVAN_FAC`,
        `PORCENTAJE_IVAR_FAC`, `TOTAL_IVAR_FAC`, `PORCENTAJE_RER_FAC`, `TOTAL_RER_FAC`,
        `TOTAL_BASEI_IVAR_FAC`, `PORCENTAJE_IVAS_FAC`, `TOTAL_IVAS_FAC`, `PORCENTAJE_RES_FAC`,
        `TOTAL_RES_FAC`, `TOTAL_BASEI_IVAS_FAC`, `PORCENTAJE_IVAE_FAC`, `TOTAL_IVAE_FAC`,
        `PORCENTAJE_REE_FAC`, `TOTAL_REE_FAC`, `TOTAL_BASEI_IVAE_FAC`, `TOTAL_BASES_FAC`,
        `TOTAL_IMPUESTOS_FAC`, `FORMA_PAGO_FAC`, `PORCENTAJE_RETENCION_FAC`, `TOTAL_RETENCION_FAC`,
        `TOTAL_LIQUIDO_FAC`, `NUMERO_FAC_ABONO_FAC`, `SERIE_FAC_ABONO_FAC`, `TEXTO_LEGAL_CLIENTE_FAC`,
        `TEXTO_LEGAL_EMPRESA_FAC`, `DOCUMENTO_FAC`, `COMENTARIOS_FAC`, `CONTADOR_LINEAS_FAC`,
        `ESCREARARTICULOS_FAC`, `ESDESCRIPCIONES_AMP_FAC`, `ESFECHADEENTREGA_FAC`, 
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 
        pUSUARIO,                    /* SIN comillas */
        pUSUARIO                     /* SIN comillas */
    FROM `fza_facturas` 
    WHERE `NUMERO_FAC`   = pidnumfactura     /* SIN comillas */
      AND `SERIE_FAC` = pidseriefactura;  /* SIN comillas */

    INSERT INTO `fza_facturas_lineas` (
        `NUMERO_FAC_FACLIN`, `SERIE_FAC_FACLIN`, `LINEA_FACLIN`, `CODIGO_ART_FACLIN`,
        `TIPO_CANTIDAD_ARTICULO_FACLIN`, `ESIMP_INCL_TARIFA_FACLIN`, `TIPO_IVA_ARTICULO_FACLIN`,
        `DESCRIPCION_ARTICULO_FACLIN`, `CANTIDAD_FACLIN`, `PRECIO_VENTA_SIVA_ARTICULO_FACLIN`,
        `PORCENTAJE_IVA_FACLIN`, `PRECIO_VENTA_CIVA_ARTICULO_FACLIN`, `TOTAL_FACLIN`,
        `INSTANTE_MODIF`, `INSTANTE_ALTA`, `USUARIO_ALTA`, `USUARIO_MODIF`
    ) 
    SELECT 
        contadorped,                 /* SIN comillas */
        pidseriefacturaabono,        /* SIN comillas */
        `LINEA_FACLIN`, `CODIGO_ART_FACLIN`, `TIPO_CANTIDAD_ARTICULO_FACLIN`,
        `ESIMP_INCL_TARIFA_FACLIN`, `TIPO_IVA_ARTICULO_FACLIN`, `DESCRIPCION_ARTICULO_FACLIN`,
        (`CANTIDAD_FACLIN` * -1),  /* ¡Perfecto esto para el abono! */
        `PRECIO_VENTA_SIVA_ARTICULO_FACLIN`, `PORCENTAJE_IVA_FACLIN`, `PRECIO_VENTA_CIVA_ARTICULO_FACLIN`,
        (`TOTAL_FACLIN` * -1),     /* ¡Perfecto esto para el abono! */
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 
        pUSUARIO,                    /* SIN comillas */
        pUSUARIO                     /* SIN comillas */
    FROM `fza_facturas_lineas` 
    WHERE `SERIE_FAC_FACLIN` = pidseriefactura   /* SIN comillas */
      AND `NUMERO_FAC_FACLIN`   = pidnumfactura;    /* SIN comillas */

    CALL `PRC_CALCULAR_FACTURA_NETOS`(pidseriefacturaabono, contadorped);
   
    COMMIT;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_CREAR_FACTURA_DUPLICADA
DROP PROCEDURE IF EXISTS `PRC_CREAR_FACTURA_DUPLICADA`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_CREAR_FACTURA_DUPLICADA`(
    IN pidseriefactura      varchar(200),
    IN pidnumfactura        varchar(200),
    IN pidseriefacturaabono varchar(200),
    IN pidcodigo_empresa    varchar(200),
    IN pfechafacturaabono   date,
    IN pUSUARIO             varchar(100),
    OUT pidnumfacturaabono  varchar(200)
)
BEGIN   
    DECLARE contadorped varchar(200);
    DECLARE pfecha date;

    /* Manejo de errores para asegurar la consistencia */    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    kk: BEGIN        
        ROLLBACK;
        RESIGNAL;
    END kk;

    START TRANSACTION;
    
    CALL PRC_GET_NEXT_CONT_FACT_SERIE(pidseriefacturaabono, 
                                      'FC', 
                                      pidcodigo_empresa,
                                      pUSUARIO,
                                      contadorped);
                                      
    SET pFecha = pfechafacturaabono;
    SET pidnumfacturaabono = contadorped;

    INSERT INTO fza_facturas (
        `NUMERO_FAC`, `SERIE_FAC`, `FECHA_FAC`, `CODIGO_EMP_FAC`,
        `RAZON_SOCIAL_EMPRESA_FAC`, `NIF_EMPRESA_FAC`, `MOVIL_EMPRESA_FAC`, `EMAIL_EMPRESA_FAC`,
        `DIRECCION1_EMPRESA_FAC`, `DIRECCION2_EMPRESA_FAC`, `POBLACION_EMPRESA_FAC`, `PROVINCIA_EMPRESA_FAC`,
        `NOMBRE_PAI_EMPRESA_FAC`, `CODIGO_PAI_EMPRESA_FAC`, `CODIGO_POSTAL_EMPRESA_FAC`, `ESRETENCIONES_EMPRESA_FAC`,
        `GRUPO_ZONA_IVA_EMPRESA_FAC`, `ESREGIMENESPECIALAGRICOLA_EMPRESA_FAC`, `CODIGO_CLI_FAC`, `RAZON_SOCIAL_CLIENTE_FAC`,
        `NIF_CLIENTE_FAC`, `MOVIL_CLIENTE_FAC`, `EMAIL_CLIENTE_FAC`, `DIRECCION1_CLIENTE_FAC`,
        `DIRECCION2_CLIENTE_FAC`, `POBLACION_CLIENTE_FAC`, `PROVINCIA_CLIENTE_FAC`, `CODIGO_POSTAL_CLIENTE_FAC`,
        `NOMBRE_PAI_CLIENTE_FAC`, `CODIGO_PAI_CLIENTE_FAC`, `ESIVA_RECARGO_CLIENTE_FAC`, `ESIVA_EXENTO_CLIENTE_FAC`,
        `ESREGIMENESPECIALAGRICOLA_CLIENTE_FAC`, `ESRETENCIONES_CLIENTE_FAC`, `TARIFA_ARTICULO_CLIENTE_FAC`, `ESIMP_INCL_TARIFA_CLIENTE_FAC`,
        `ESINTRACOMUNITARIO_CLIENTE_FAC`, `ESIRPF_IMP_INCL_ZONA_IVA_FAC`, `ESAPLICA_RE_ZONA_IVA_FAC`, `ESIVAAGRICOLA_ZONA_IVA_FAC`,
        `PALABRA_REPORTS_ZONA_IVA_FAC`, `CODIGO_IVA_FAC`, `ESVENTA_ACTIVO_FIJO_FAC`, `PORCENTAJE_IVAN_FAC`,
        `TOTAL_IVAN_FAC`, `PORCENTAJE_REN_FAC`, `TOTAL_REN_FAC`, `TOTAL_BASEI_IVAN_FAC`,
        `PORCENTAJE_IVAR_FAC`, `TOTAL_IVAR_FAC`, `PORCENTAJE_RER_FAC`, `TOTAL_RER_FAC`,
        `TOTAL_BASEI_IVAR_FAC`, `PORCENTAJE_IVAS_FAC`, `TOTAL_IVAS_FAC`, `PORCENTAJE_RES_FAC`,
        `TOTAL_RES_FAC`, `TOTAL_BASEI_IVAS_FAC`, `PORCENTAJE_IVAE_FAC`, `TOTAL_IVAE_FAC`,
        `PORCENTAJE_REE_FAC`, `TOTAL_REE_FAC`, `TOTAL_BASEI_IVAE_FAC`, `TOTAL_BASES_FAC`,
        `TOTAL_IMPUESTOS_FAC`, `FORMA_PAGO_FAC`, `PORCENTAJE_RETENCION_FAC`, `TOTAL_RETENCION_FAC`,
        `TOTAL_LIQUIDO_FAC`, `NUMERO_FAC_ABONO_FAC`, `SERIE_FAC_ABONO_FAC`, `TEXTO_LEGAL_CLIENTE_FAC`,
        `TEXTO_LEGAL_EMPRESA_FAC`, `DOCUMENTO_FAC`, `COMENTARIOS_FAC`, `CONTADOR_LINEAS_FAC`,
        `ESCREARARTICULOS_FAC`, `ESDESCRIPCIONES_AMP_FAC`, `ESFECHADEENTREGA_FAC`, `INSTANTE_MODIF`,
        `INSTANTE_ALTA`, `USUARIO_ALTA`, `USUARIO_MODIF`
    )
    SELECT 
        contadorped,                  
        pidseriefacturaabono,         
        pFecha,                       
        `CODIGO_EMP_FAC`, `RAZON_SOCIAL_EMPRESA_FAC`, `NIF_EMPRESA_FAC`, `MOVIL_EMPRESA_FAC`, `EMAIL_EMPRESA_FAC`,
        `DIRECCION1_EMPRESA_FAC`, `DIRECCION2_EMPRESA_FAC`, `POBLACION_EMPRESA_FAC`, `PROVINCIA_EMPRESA_FAC`,
        `NOMBRE_PAI_EMPRESA_FAC`, `CODIGO_PAI_EMPRESA_FAC`, `CODIGO_POSTAL_EMPRESA_FAC`, `ESRETENCIONES_EMPRESA_FAC`,
        `GRUPO_ZONA_IVA_EMPRESA_FAC`, `ESREGIMENESPECIALAGRICOLA_EMPRESA_FAC`, `CODIGO_CLI_FAC`, `RAZON_SOCIAL_CLIENTE_FAC`,
        `NIF_CLIENTE_FAC`, `MOVIL_CLIENTE_FAC`, `EMAIL_CLIENTE_FAC`, `DIRECCION1_CLIENTE_FAC`,
        `DIRECCION2_CLIENTE_FAC`, `POBLACION_CLIENTE_FAC`, `PROVINCIA_CLIENTE_FAC`, `CODIGO_POSTAL_CLIENTE_FAC`,
        `NOMBRE_PAI_CLIENTE_FAC`, `CODIGO_PAI_CLIENTE_FAC`, `ESIVA_RECARGO_CLIENTE_FAC`, `ESIVA_EXENTO_CLIENTE_FAC`,
        `ESREGIMENESPECIALAGRICOLA_CLIENTE_FAC`, `ESRETENCIONES_CLIENTE_FAC`, `TARIFA_ARTICULO_CLIENTE_FAC`, `ESIMP_INCL_TARIFA_CLIENTE_FAC`,
        `ESINTRACOMUNITARIO_CLIENTE_FAC`, `ESIRPF_IMP_INCL_ZONA_IVA_FAC`, `ESAPLICA_RE_ZONA_IVA_FAC`, `ESIVAAGRICOLA_ZONA_IVA_FAC`,
        `PALABRA_REPORTS_ZONA_IVA_FAC`, `CODIGO_IVA_FAC`, `ESVENTA_ACTIVO_FIJO_FAC`, `PORCENTAJE_IVAN_FAC`,
        `TOTAL_IVAN_FAC`, `PORCENTAJE_REN_FAC`, `TOTAL_REN_FAC`, `TOTAL_BASEI_IVAN_FAC`,
        `PORCENTAJE_IVAR_FAC`, `TOTAL_IVAR_FAC`, `PORCENTAJE_RER_FAC`, `TOTAL_RER_FAC`,
        `TOTAL_BASEI_IVAR_FAC`, `PORCENTAJE_IVAS_FAC`, `TOTAL_IVAS_FAC`, `PORCENTAJE_RES_FAC`,
        `TOTAL_RES_FAC`, `TOTAL_BASEI_IVAS_FAC`, `PORCENTAJE_IVAE_FAC`, `TOTAL_IVAE_FAC`,
        `PORCENTAJE_REE_FAC`, `TOTAL_REE_FAC`, `TOTAL_BASEI_IVAE_FAC`, `TOTAL_BASES_FAC`,
        `TOTAL_IMPUESTOS_FAC`, `FORMA_PAGO_FAC`, `PORCENTAJE_RETENCION_FAC`, `TOTAL_RETENCION_FAC`,
        `TOTAL_LIQUIDO_FAC`, `NUMERO_FAC_ABONO_FAC`, `SERIE_FAC_ABONO_FAC`, `TEXTO_LEGAL_CLIENTE_FAC`,
        `TEXTO_LEGAL_EMPRESA_FAC`, `DOCUMENTO_FAC`, `COMENTARIOS_FAC`, `CONTADOR_LINEAS_FAC`,
        `ESCREARARTICULOS_FAC`, `ESDESCRIPCIONES_AMP_FAC`, `ESFECHADEENTREGA_FAC`, 
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 
        pUSUARIO,                     
        pUSUARIO                      
    FROM `fza_facturas` 
    WHERE `NUMERO_FAC` = pidnumfactura     
      AND `SERIE_FAC` = pidseriefactura;  

    INSERT INTO fza_facturas_lineas (
        `NUMERO_FAC_FACLIN`, `SERIE_FAC_FACLIN`, `LINEA_FACLIN`, `CODIGO_ART_FACLIN`,
        `TIPO_CANTIDAD_ARTICULO_FACLIN`, `ESIMP_INCL_TARIFA_FACLIN`, `TIPO_IVA_ARTICULO_FACLIN`,
        `DESCRIPCION_ARTICULO_FACLIN`, `CANTIDAD_FACLIN`, `PRECIO_VENTA_SIVA_ARTICULO_FACLIN`,
        `PORCENTAJE_IVA_FACLIN`, `PRECIO_VENTA_CIVA_ARTICULO_FACLIN`, `TOTAL_FACLIN`,
        `INSTANTE_MODIF`, `INSTANTE_ALTA`, `USUARIO_ALTA`, `USUARIO_MODIF`
    )
    SELECT 
        contadorped,                  
        pidseriefacturaabono,         
        `LINEA_FACLIN`, `CODIGO_ART_FACLIN`,
        `TIPO_CANTIDAD_ARTICULO_FACLIN`, `ESIMP_INCL_TARIFA_FACLIN`, `TIPO_IVA_ARTICULO_FACLIN`,
        `DESCRIPCION_ARTICULO_FACLIN`, `CANTIDAD_FACLIN`, `PRECIO_VENTA_SIVA_ARTICULO_FACLIN`,
        `PORCENTAJE_IVA_FACLIN`, `PRECIO_VENTA_CIVA_ARTICULO_FACLIN`, `TOTAL_FACLIN`,
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 
        pUSUARIO,                     
        pUSUARIO                      
    FROM `fza_facturas_lineas`                            
    WHERE `SERIE_FAC_FACLIN` = pidseriefactura   
      AND `NUMERO_FAC_FACLIN` = pidnumfactura;      
      
    COMMIT;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_CREAR_METADATOS
DROP PROCEDURE IF EXISTS `PRC_CREAR_METADATOS`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_CREAR_METADATOS`(IN `pDATABASENAME` varchar(100))
BEGIN

    /* Manejo de errores para asegurar la consistencia */
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;
START TRANSACTION;
  DROP TABLE IF EXISTS `fza_metadatos`;
  CREATE OR REPLACE TABLE `fza_metadatos`  (
    `CODIGO_META_META` int(20) NOT NULL AUTO_INCREMENT,
    `NOMBRE_META_META` varchar(100) CHARACTER SET utf8mb4 
                               COLLATE utf8mb4_spanish_ci NOT NULL,
    `PARENT_META` varchar(20) CHARACTER SET utf8mb4 
                               COLLATE utf8mb4_spanish_ci NOT NULL,
    PRIMARY KEY (`CODIGO_META_META`) USING BTREE
  ) ENGINE = InnoDB AUTO_INCREMENT = 4 CHARACTER SET = utf8mb4 
             COLLATE = utf8mb4_spanish_ci ROW_FORMAT = Dynamic;
  INSERT INTO `fza_metadatos` (`PARENT_META`, `NOMBRE_META_META`)
  SELECT '1' AS `PARENT_META`,
         `table_name` as `NOMBRE_META_META`
    FROM `information_schema`.`TABLES` 
   WHERE `table_schema` = `pDATABASENAME`  
     AND `table_type` = 'BASE TABLE';
  INSERT INTO `fza_metadatos` (`PARENT_META`, `NOMBRE_META_META`)    
  SELECT '2' AS `PARENT_META`,
         `table_name` as `NOMBRE_META_META`
    FROM `information_schema`.`TABLES` 
   WHERE `table_schema` = `pDATABASENAME`
     AND `table_type` = 'VIEW';
   INSERT INTO `fza_metadatos` (`PARENT_META`, `NOMBRE_META_META`) 
   SELECT '3' AS `PARENT_META`,
          `SPECIFIC_NAME` AS `NOMBRE_META_META`
     FROM `information_schema`.`ROUTINES` 
    WHERE `ROUTINE_SCHEMA` = pDATABASENAME  
      AND `ROUTINE_TYPE` = 'PROCEDURE';     
   
   INSERT INTO `fza_metadatos` (`CODIGO_META_META`, 
                                `PARENT_META`, 
                                `NOMBRE_META_META`) 
                        VALUES (1, '-1','Tablas');  
   INSERT INTO `fza_metadatos` (`CODIGO_META_META`, 
                                `PARENT_META`, 
                                `NOMBRE_META_META`) 
                        VALUES (2, '-1','Vistas');
   INSERT INTO `fza_metadatos` (`CODIGO_META_META`, 
                                `PARENT_META`, 
                                `NOMBRE_META_META`) 
                        VALUES (3, '-1','Procedimientos');
COMMIT;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_CREAR_RECIBOS_FACTURA
DROP PROCEDURE IF EXISTS `PRC_CREAR_RECIBOS_FACTURA`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_CREAR_RECIBOS_FACTURA`(
    IN pSERIE_FACTURA varchar(12),
    IN pNRO_FACTURA   varchar(12),
    IN pUSUARIO       varchar(100)
)
BEGIN
    DECLARE pCODIGO_FORMAPAGO VARCHAR(20);
    DECLARE pFORMA_PAGO_FACTURA VARCHAR(100);
    DECLARE pN_PLAZOS int(10); 
    DECLARE I int(10);
    DECLARE pDIAS_ENTRE_PLAZOS int(10);
    DECLARE pPORCEN_ANTICIPO decimal(5,2);
    DECLARE pCODIGO_CLIENTE  varchar(20);
    DECLARE pIBAN varchar(34);
    DECLARE pRAZONSOCIAL_CLIENTE varchar(200);
    DECLARE pDIRECCION1_CLIENTE  varchar(200);
    DECLARE pPOBLACION_CLIENTE  varchar(200);
    DECLARE pPOBLACION_EMPRESA varchar(200);
    DECLARE pPROVINCIA_CLIENTE  varchar(200);
    DECLARE pCPOSTAL_CLIENTE  varchar(15);
    DECLARE pIMPORTE_LETRA  varchar(150);
    DECLARE pTOTAL_LIQUIDO_FACTURA decimal(18,6);
    DECLARE pIMPORTE_RECIBO decimal(18,6);
    DECLARE pIMPORTE_RESTO decimal(18,6);
    DECLARE pIMPORTE_ANTICIPO decimal(18,6);
    DECLARE pFECHA_VENCIMIENTO date;
    DECLARE pFECHA_FACTURA date;
    DECLARE finished INTEGER DEFAULT 0;

    /* Manejo de errores para asegurar la consistencia */
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    kk: BEGIN
        ROLLBACK;
        RESIGNAL;
    END kk;

    START TRANSACTION;

    /* Limpiamos los recibos existentes de esta factura */
    DELETE FROM fza_recibos 
    WHERE NUMERO_FAC_REC = pNRO_FACTURA
      AND SERIE_FAC_REC = pSERIE_FACTURA;   

    /* Obtenemos los datos de la factura */
    SELECT FORMA_PAGO_FAC, 
           CODIGO_CLI_FAC,
           TOTAL_LIQUIDO_FAC,
           RAZON_SOCIAL_CLIENTE_FAC,
           DIRECCION1_CLIENTE_FAC,
           POBLACION_CLIENTE_FAC,
           PROVINCIA_CLIENTE_FAC,
           CODIGO_POSTAL_CLIENTE_FAC,
           FECHA_FAC,
           POBLACION_EMPRESA_FAC
    INTO pFORMA_PAGO_FACTURA,
         pCODIGO_CLIENTE,
         pTOTAL_LIQUIDO_FACTURA,
         pRAZONSOCIAL_CLIENTE,
         pDIRECCION1_CLIENTE,
         pPOBLACION_CLIENTE,
         pPROVINCIA_CLIENTE,
         pCPOSTAL_CLIENTE,
         pFECHA_FACTURA,
         pPOBLACION_EMPRESA
    FROM fza_facturas
    WHERE SERIE_FAC = pSERIE_FACTURA
      AND NUMERO_FAC = pNRO_FACTURA;
         
    /* Obtenemos el IBAN del cliente */
    SELECT IBAN_CLI 
    INTO pIBAN
    FROM fza_clientes
    WHERE CODIGO_CLI_CLI = pCODIGO_CLIENTE;
         
    /* Comprobamos que existe la forma de pago */
    IF( EXISTS( SELECT *
                FROM fza_formas_pago
                WHERE CODIGO_FP_FP = pFORMA_PAGO_FACTURA) ) THEN
        
        /* Obtenemos los detalles de la forma de pago */
        SELECT CODIGO_FP_FP, 
               N_PLAZOS_FORMA_PAGO_FP, 
               N_DIAS_ENTRE_PLAZOS_FORMA_PAGO_FP, 
               PORCENTAJE_ANTICIPO_FORMA_PAGO_FP 
        INTO pCODIGO_FORMAPAGO,
             pN_PLAZOS, 
             pDIAS_ENTRE_PLAZOS,  
             pPORCEN_ANTICIPO
        FROM fza_formas_pago
        WHERE CODIGO_FP_FP = pFORMA_PAGO_FACTURA;
            
        /* Si es anticipo del 100% (un solo recibo al contado) */
        IF (pPORCEN_ANTICIPO = 100) THEN
        
            CALL PRC_GET_NUMEROS_A_LETRAS(pTOTAL_LIQUIDO_FACTURA, pIMPORTE_LETRA);
            
            INSERT INTO fza_recibos (
                NUMERO_FAC_REC,
                SERIE_FAC_REC,
                NUMERO_PLAZO_REC,
                FORMA_PAGO_ORIGEN_RECIBO_REC,
                FORMA_PAGO_DESCRIPCION_ORIGEN_RECIBO_REC,                               
                EUROS_RECIBO_REC,
                ESTADO_RECIBO_REC,
                FECHA_EXPEDICION_RECIBO_REC,
                FECHA_VENCIMIENTO_RECIBO_REC,
                IBAN_CLI_REC,
                FECHA_PAGO_RECIBO_REC,
                LOCALIDAD_EXPEDICION_RECIBO_REC,
                CODIGO_CLI_REC,
                RAZON_SOCIAL_CLI_REC,
                DIRECCION1_CLIENTE_RECIBO_REC,
                POBLACION_CLI_REC,
                PROVINCIA_CLI_REC,
                CODIGO_POSTAL_CLI_REC,
                IMPORTE_LETRA_RECIBO_REC,
                INSTANTE_ALTA,
                INSTANTE_MODIF,
                USUARIO_ALTA,
                USUARIO_MODIF    
            ) VALUES ( 
                pNRO_FACTURA,
                pSERIE_FACTURA,
                1,
                pCODIGO_FORMAPAGO,
                pFORMA_PAGO_FACTURA,
                pTOTAL_LIQUIDO_FACTURA,
                'Pagado',
                pFECHA_FACTURA,
                pFECHA_FACTURA,
                pIBAN,
                pFECHA_FACTURA,
                pPOBLACION_EMPRESA,
                pCODIGO_CLIENTE,
                pRAZONSOCIAL_CLIENTE,
                pDIRECCION1_CLIENTE,
                pPOBLACION_CLIENTE,
                pPROVINCIA_CLIENTE,
                pCPOSTAL_CLIENTE,
                pIMPORTE_LETRA,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                pUSUARIO,
                pUSUARIO
            );
            
        ELSE 
            /* Si va por plazos */
            IF (pN_PLAZOS >= 1) THEN
                SET I = 1;
                
                WHILE (I <= pN_PLAZOS) DO
                
                    IF (I = 1) THEN 
                        SET pFECHA_VENCIMIENTO = ADDDATE(pFECHA_FACTURA, INTERVAL pDIAS_ENTRE_PLAZOS DAY);
                        SET pIMPORTE_ANTICIPO = pTOTAL_LIQUIDO_FACTURA * (pPORCEN_ANTICIPO / 100);
                        SET pIMPORTE_RESTO = (pTOTAL_LIQUIDO_FACTURA - pIMPORTE_ANTICIPO);
                    END IF;                                  
                    
                    IF ((I = 1) AND (pPORCEN_ANTICIPO > 0)) THEN
                        SET pIMPORTE_RECIBO = pIMPORTE_ANTICIPO;
                    ELSE
                        IF pN_PLAZOS > 1 THEN 
                            SET pIMPORTE_RECIBO = pIMPORTE_RESTO / (pN_PLAZOS);
                        ELSE
                            SET pIMPORTE_RECIBO = pIMPORTE_RESTO;
                        END IF;
                    END IF;
                    
                    CALL PRC_GET_NUMEROS_A_LETRAS(pIMPORTE_RECIBO, pIMPORTE_LETRA);   
                    
                    IF (I <> 1) THEN
                        SET pFECHA_VENCIMIENTO = ADDDATE(pFECHA_VENCIMIENTO, INTERVAL pDIAS_ENTRE_PLAZOS DAY);
                    END IF;
                    
                    INSERT INTO fza_recibos (
                        NUMERO_FAC_REC,
                        SERIE_FAC_REC,
                        NUMERO_PLAZO_REC,
                        FORMA_PAGO_ORIGEN_RECIBO_REC,                                                 
                        FORMA_PAGO_DESCRIPCION_ORIGEN_RECIBO_REC,
                        EUROS_RECIBO_REC,
                        ESTADO_RECIBO_REC,
                        FECHA_EXPEDICION_RECIBO_REC,
                        FECHA_VENCIMIENTO_RECIBO_REC,
                        IBAN_CLI_REC,
                        FECHA_PAGO_RECIBO_REC,
                        LOCALIDAD_EXPEDICION_RECIBO_REC,
                        CODIGO_CLI_REC,
                        RAZON_SOCIAL_CLI_REC,
                        DIRECCION1_CLIENTE_RECIBO_REC,
                        POBLACION_CLI_REC,
                        PROVINCIA_CLI_REC,
                        CODIGO_POSTAL_CLI_REC,
                        IMPORTE_LETRA_RECIBO_REC,   
                        INSTANTE_ALTA,
                        INSTANTE_MODIF,
                        USUARIO_ALTA,
                        USUARIO_MODIF    
                    ) VALUES (  
                        pNRO_FACTURA,
                        pSERIE_FACTURA,
                        I,
                        pCODIGO_FORMAPAGO,                                      
                        pFORMA_PAGO_FACTURA,
                        pIMPORTE_RECIBO,
                        'Emitido',
                        pFECHA_FACTURA,
                        pFECHA_VENCIMIENTO,
                        pIBAN,
                        NULL,
                        pPOBLACION_EMPRESA,
                        pCODIGO_CLIENTE,
                        pRAZONSOCIAL_CLIENTE,
                        pDIRECCION1_CLIENTE,
                        pPOBLACION_CLIENTE,
                        pPROVINCIA_CLIENTE,
                        pCPOSTAL_CLIENTE,
                        pIMPORTE_LETRA,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        pUSUARIO,
                        pUSUARIO
                    );
                    SET I = I + 1; 
                END WHILE;
            END IF;
        END IF;
    END IF;
    COMMIT;    
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_CREAR_TRASPASO
DROP PROCEDURE IF EXISTS `PRC_CREAR_TRASPASO`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_CREAR_TRASPASO`(IN pUsuario VARCHAR(50),
    IN pEmpresa VARCHAR(20),
    IN pAlmacenOrigen VARCHAR(10),
    IN pAlmacenDestino VARCHAR(10),
    IN pSku VARCHAR(50),
    IN pCantidad DECIMAL(19,6))
BEGIN
    DECLARE vSerie VARCHAR(20) DEFAULT 'TRAS';
    DECLARE vNroDoc VARCHAR(20);
    
    /* Generamos un número de documento único basado en la fecha y hora (simplificado) */
    SET vNroDoc = DATE_FORMAT(NOW(), '%Y%m%d%H%i%s');

    START TRANSACTION;

    /* 1. SALIDA DEL ORIGEN (Resta stock en Origen) */
    INSERT INTO `fza_movimientos_almacen` 
    (TIPO_DOC_MOV, SERIE_DOC_MOV, NUMERO_DOC_MOV, LINEA_MOV, CODIGO_EMP_MOV, 
     CODIGO_ALM_MOV, CODIGO_ALM_CONTRA_MOV, FECHA_MOV, 
     CODIGO_UNIDAD_MOV, TIPO_MOV, CANTIDAD_MOV, 
     DESCRIPCION_ARTICULO_MOV, USUARIO_ALTA, USUARIO_MODIF)
    VALUES 
    ('TR', vSerie, vNroDoc, '001', pEmpresa, 
     pAlmacenOrigen, pAlmacenDestino, NOW(), 
     pSku, 'S', pCantidad, 
     CONCAT('Traspaso a ', pAlmacenDestino), pUsuario, pUsuario);

    /* 2. ENTRADA EN DESTINO (Suma stock en Destino) */
    /* Referenciamos al movimiento anterior para trazabilidad */
    INSERT INTO `fza_movimientos_almacen` 
    (TIPO_DOC_MOV, SERIE_DOC_MOV, NUMERO_DOC_MOV, LINEA_MOV, CODIGO_EMP_MOV, 
     CODIGO_ALM_MOV, CODIGO_ALM_CONTRA_MOV, FECHA_MOV, 
     CODIGO_UNIDAD_MOV, TIPO_MOV, CANTIDAD_MOV, 
     DESCRIPCION_ARTICULO_MOV, USUARIO_ALTA, USUARIO_MODIF,
     TIPO_DOC_REF_MOV, SERIE_DOC_REF_MOV, NUMERO_DOC_REF_MOV, LINEA_REF_MOV)
    VALUES 
    ('TR', vSerie, vNroDoc, '002', pEmpresa, 
     pAlmacenDestino, pAlmacenOrigen, NOW(), 
     pSku, 'E', pCantidad, 
     CONCAT('Traspaso desde ', pAlmacenOrigen), pUsuario, pUsuario,
     'TR', vSerie, vNroDoc, '001');

    COMMIT;
    
    SELECT CONCAT('Traspaso realizado. Doc: ', vSerie, '-', vNroDoc) as MENSAJE;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_FNC_GET_NEXT_LINEA_FACTURA
DROP PROCEDURE IF EXISTS `PRC_FNC_GET_NEXT_LINEA_FACTURA`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_FNC_GET_NEXT_LINEA_FACTURA`(
    IN  `pnumfac` VARCHAR(12), 
    IN  `pserie`  VARCHAR(12), 
    OUT `presul`  VARCHAR(3)
)
BEGIN    
    DECLARE v_NextValue BIGINT;

    /* Manejo de errores para asegurar la consistencia (etiqueta para Uniscript) */    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    kk: BEGIN        
        ROLLBACK;
        RESIGNAL;
    END kk;
    
    START TRANSACTION;
    
    /* 1 y 4 FUSIONADOS: Bloqueamos, calculamos y actualizamos de golpe */
    UPDATE `fza_facturas`
    SET `CONTADOR_LINEAS_FAC` = LPAD(
        LAST_INSERT_ID(
            CASE 
                /* Si es la primera vez (nulo, vacío o 0), preparamos el TERRENO para el futuro (20) */
                WHEN `CONTADOR_LINEAS_FAC` IS NULL 
                  OR `CONTADOR_LINEAS_FAC` = '' 
                  OR `CONTADOR_LINEAS_FAC` = '0' THEN 20
                /* Si ya tiene valor, le sumamos 10 */
                ELSE CAST(`CONTADOR_LINEAS_FAC` AS UNSIGNED) + 10
            END
        ), 3, '0'
    )
    WHERE `SERIE_FAC` = pserie 
      AND `NUMERO_FAC`   = pnumfac;
    
    /* Comprobamos si la factura existía y se actualizó */
    IF ROW_COUNT() > 0 THEN
        /* Si hemos guardado 20, nos toca devolver 10 ('010')
           Si hemos guardado 30, nos toca devolver 20 ('020')
        */
        SET v_NextValue = LAST_INSERT_ID() - 10;
        SET `presul` = LPAD(v_NextValue, 3, '0');
    ELSE
        /* Comportamiento de seguridad igual al tuyo: 
           Si por algún motivo la factura aún no existe en fza_facturas, devolvemos '010' 
        */
        SET `presul` = '010';
    END IF;
    
    COMMIT;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_FNC_GET_NEXT_NRO_DOC
DROP PROCEDURE IF EXISTS `PRC_FNC_GET_NEXT_NRO_DOC`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_FNC_GET_NEXT_NRO_DOC`(IN  `ptipodoc` VARCHAR(8), 
                                             INOUT `ppresul`   BIGINT)
BEGIN

    /* Manejo de errores para asegurar la consistencia */
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    kk: BEGIN
        ROLLBACK;
        RESIGNAL;
    END kk;
START TRANSACTION;
UPDATE `fza_contadores`
   SET `CON` = CON + 1
 WHERE `SERIE_CON` = '-'
   AND `TIPO_DOC_CON` = `pTipoDoc`;
  SET `ppresul` = (SELECT `CON` - 1
                     FROM `fza_contadores`
                    WHERE `SERIE_CON` = '-'
                      AND `DEFAULT_CON` = 'S'
                      AND `TIPO_DOC_CON` = `pTipoDoc` 
                    LIMIT 1);
COMMIT;

END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_FNC_GET_PRECIO_ARTICULO_FECHA
DROP PROCEDURE IF EXISTS `PRC_FNC_GET_PRECIO_ARTICULO_FECHA`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_FNC_GET_PRECIO_ARTICULO_FECHA`(
  in pCODIGO_ARTICULO varchar(20),
  in pFECHA date,
  inout pPRECIOSALIDA_TARIFA decimal(19,6),
  inout pPRECIOFINAL_TARIFA decimal(19,6),
  inout pPORCEN_DTO_TARIFA decimal(19,6),
  inout pPRECIO_DTO_TARIFA decimal(19,6))
BEGIN
  SELECT PRECIO_SALIDA_ARTTAR,
    PRECIO_FINAL_ARTTAR,
    PORCENTAJE_DTO_ARTTAR,
    PRECIO_DTO_ARTTAR
    INTO
    pPRECIOSALIDA_TARIFA,
    pPRECIOFINAL_TARIFA,
    pPORCEN_DTO_TARIFA,
    pPRECIO_DTO_TARIFA
    FROM fza_articulos_tarifas
    WHERE CODIGO_ART_ARTTAR = pCODIGO_ARTICULO
    AND FECHA_DESDE_ARTTAR <= pFECHA
    AND (FECHA_HASTA_ARTTAR IS NULL OR
    FECHA_HASTA_ARTTAR >= pFECHA);
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_FNC_GET_SERIE_TIPODOC
DROP PROCEDURE IF EXISTS `PRC_FNC_GET_SERIE_TIPODOC`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_FNC_GET_SERIE_TIPODOC`(IN `ptipodoc` VARCHAR(8), 
                                                OUT `presul` VARCHAR(3))
BEGIN


DECLARE pserie varchar(3);
SET pserie = (select SERIE_CON
FROM fza_contadores
  where DEFAULT_CON = 'S'
and TIPO_DOC_CON = ptipodoc);
IF (pserie IS NULL) THEN
   SET presul = '-';
ELSE
   SET presul = pserie;
END IF;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_FZA_AJUSTAR_ACUMULADO_STK
DROP PROCEDURE IF EXISTS `PRC_FZA_AJUSTAR_ACUMULADO_STK`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_FZA_AJUSTAR_ACUMULADO_STK`(
    IN `p_TIPO_DOC_MOV` VARCHAR(20),
    IN `p_TIPO_MOV` VARCHAR(1),
    IN `p_CODIGO_ALM` VARCHAR(10),
    IN `p_CODIGO_UNIDAD` VARCHAR(50),
    IN `p_CANTIDAD` DECIMAL(19,6),
    IN `p_VALOR` DECIMAL(19,6),
    IN `p_MULT` INT  /* +1 para sumar, -1 para restar */
)
BEGIN
    DECLARE v_signo INT;
    SET v_signo = IF(p_TIPO_MOV = 'E', 1, -1);
    UPDATE fza_articulos_stockactual SET
        CANTIDAD_STK      = CANTIDAD_STK      + (v_signo * p_MULT * p_CANTIDAD),
        VALOR_TOTAL_STK   = VALOR_TOTAL_STK   + (v_signo * p_MULT * p_VALOR),
        PRECIO_MEDIO_STK  = IF(CANTIDAD_STK > 0, VALOR_TOTAL_STK / CANTIDAD_STK, 0),
        INSTANTE_MODIF    = NOW(),
        CANTIDAD_ENT_COMPRA_STK = CANTIDAD_ENT_COMPRA_STK +
            IF(p_TIPO_DOC_MOV='AC' AND p_TIPO_MOV='E', p_MULT * p_CANTIDAD, 0),
        CANTIDAD_ENT_TRASPASO_STK = CANTIDAD_ENT_TRASPASO_STK +
            IF(p_TIPO_DOC_MOV IN ('TR','AT') AND p_TIPO_MOV='E', p_MULT * p_CANTIDAD, 0),
        CANTIDAD_SAL_TRASPASO_STK = CANTIDAD_SAL_TRASPASO_STK +
            IF(p_TIPO_DOC_MOV IN ('TR','AT') AND p_TIPO_MOV='S', p_MULT * p_CANTIDAD, 0),
        CANTIDAD_ENT_DEPOSITO_STK = CANTIDAD_ENT_DEPOSITO_STK +
            IF(p_TIPO_DOC_MOV='DP' AND p_TIPO_MOV='E', p_MULT * p_CANTIDAD, 0),
        CANTIDAD_SAL_DEPOSITO_STK = CANTIDAD_SAL_DEPOSITO_STK +
            IF(p_TIPO_DOC_MOV='DP' AND p_TIPO_MOV='S', p_MULT * p_CANTIDAD, 0),
        CANTIDAD_SAL_VENTA_STK = CANTIDAD_SAL_VENTA_STK +
            IF(p_TIPO_DOC_MOV='VE' AND p_TIPO_MOV='S', p_MULT * p_CANTIDAD, 0),
        CANTIDAD_ENT_REGULAR_STK = CANTIDAD_ENT_REGULAR_STK +
            IF(p_TIPO_DOC_MOV='IN' AND p_TIPO_MOV='E', p_MULT * p_CANTIDAD, 0),
        CANTIDAD_SAL_ALBVENTA_STK = CANTIDAD_SAL_ALBVENTA_STK +
            IF(p_TIPO_DOC_MOV='AV' AND p_TIPO_MOV='S', p_MULT * p_CANTIDAD, 0),
        CANTIDAD_ENT_ALBENTRADA_STK = CANTIDAD_ENT_ALBENTRADA_STK +
            IF(p_TIPO_DOC_MOV='AE' AND p_TIPO_MOV='E', p_MULT * p_CANTIDAD, 0)
    WHERE CODIGO_ALM_STK = p_CODIGO_ALM
      AND CODIGO_UNIDAD_STK = p_CODIGO_UNIDAD;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_FZA_DEPOSITOS_INSERT
DROP PROCEDURE IF EXISTS `PRC_FZA_DEPOSITOS_INSERT`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_FZA_DEPOSITOS_INSERT`(
    IN p_ID_DEP VARCHAR(20),
    IN p_EMP VARCHAR(20),
    IN p_ALM_DEP VARCHAR(10),
    IN p_CLI VARCHAR(20),
    IN p_ART VARCHAR(50),
    IN p_SKU VARCHAR(50),
    IN p_PRECIO DECIMAL(19,6),
    IN p_CANTIDAD DECIMAL(19,6),
    IN p_ANTICIPO DECIMAL(19,6),
    IN p_TIPOIVA CHAR(1),
    IN p_PORCIVA DECIMAL(19,6),
    IN p_IMPINCL CHAR(1),
    IN p_CAJA VARCHAR(10),        /* NUEVO */
    IN p_NUMOP VARCHAR(20),       /* NUEVO */
    IN p_USUARIO VARCHAR(100)
)
BEGIN
    DECLARE v_deuda_nueva DECIMAL(19,6) DEFAULT 0;

    /* Manejo de errores para asegurar la consistencia */
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    kk: BEGIN
        ROLLBACK;
        RESIGNAL;
    END kk;

    START TRANSACTION;

    /* 1. Insertamos el depósito en la tabla */
    INSERT INTO fza_depositos_cliente (
        ID_DEPOSITO_DEP, CODIGO_EMP_DEP, CODIGO_ALM_DEP,
        CODIGO_CLI_DEP, CODIGO_ART_DEP, CODIGO_UNIDAD_DEP,
        ESTADO_DEP, PRECIO_VENTA_DEP, CANTIDAD_PENDIENTE_DEP, IMPORTE_ANTICIPO_DEP,
        TIPO_IVA_DEP, PORCENTAJE_IVA_DEP, ESIMP_INCL_DEP,
        CODIGO_CAJA_DEP, NUMERO_OPERACION_DEP, /* NUEVOS CAMPOS */
        INSTANTE_ALTA, USUARIO_ALTA, INSTANTE_MODIF, USUARIO_MODIF
    ) VALUES (
        p_ID_DEP, p_EMP, p_ALM_DEP,
        p_CLI, p_ART, p_SKU, 
        'PENDIENTE', p_PRECIO, p_CANTIDAD, p_ANTICIPO,
        p_TIPOIVA, p_PORCIVA, p_IMPINCL,
        p_CAJA, p_NUMOP, /* NUEVOS VALORES */
        NOW(), p_USUARIO, NOW(), p_USUARIO
    );

    /* 2. Calculamos la nueva deuda (Lógica adaptada de tu trigger) */
    SET v_deuda_nueva = (p_PRECIO * COALESCE(p_CANTIDAD, 1)) - COALESCE(p_ANTICIPO, 0);

    /* 3. Si hay deuda generada, actualizamos el cliente */
    IF v_deuda_nueva > 0 AND p_CLI IS NOT NULL THEN
        UPDATE fza_clientes 
           SET TOTAL_DEUDA_CLI = COALESCE(TOTAL_DEUDA_CLI, 0) + v_deuda_nueva 
         WHERE CODIGO_CLI_CLI = p_CLI;
    END IF;

    COMMIT;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_FZA_DEPOSITOS_UPDATE
DROP PROCEDURE IF EXISTS `PRC_FZA_DEPOSITOS_UPDATE`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_FZA_DEPOSITOS_UPDATE`(
    IN SKU VARCHAR(50),
    IN CLI VARCHAR(20),
    IN ESTADO VARCHAR(15),
    IN INC_ANTICIPO DECIMAL(19,6),
    IN USUARIO VARCHAR(100),
    OUT P_ID_DEPOSITO VARCHAR(20)
)
BEGIN
    /* Variables para almacenar los datos actuales del depósito */
    DECLARE v_ID_DEPOSITO VARCHAR(20);
    DECLARE v_OLD_ESTADO VARCHAR(15);
    DECLARE v_OLD_PRECIO DECIMAL(19,6);
    DECLARE v_OLD_CANTIDAD_PTE DECIMAL(19,6);
    DECLARE v_OLD_ANTICIPO DECIMAL(19,6);

    /* Variables para calcular los nuevos valores y la deuda */
    DECLARE v_NUEVO_ESTADO VARCHAR(15);
    DECLARE v_NUEVO_ANTICIPO DECIMAL(19,6);
    DECLARE v_deuda_antigua DECIMAL(19,6) DEFAULT 0;
    DECLARE v_deuda_nueva DECIMAL(19,6) DEFAULT 0;
    DECLARE v_diferencia DECIMAL(19,6) DEFAULT 0;

    /* Manejador de errores: Deshace todo si algo falla */
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;

    /* 1. Buscar el depósito que coincide con el Cliente y el SKU */
    /* Buscamos el que esté 'PENDIENTE' y lo bloqueamos para evitar concurrencia */
    SELECT ID_DEPOSITO_DEP, ESTADO_DEP, PRECIO_VENTA_DEP, CANTIDAD_PENDIENTE_DEP, IMPORTE_ANTICIPO_DEP
      INTO v_ID_DEPOSITO, v_OLD_ESTADO, v_OLD_PRECIO, v_OLD_CANTIDAD_PTE, v_OLD_ANTICIPO
      FROM fza_depositos_cliente
     WHERE CODIGO_CLI_DEP = CLI
       AND CODIGO_UNIDAD_DEP = SKU
       AND ESTADO_DEP = 'PENDIENTE'
     LIMIT 1 /* Por seguridad, nos quedamos con uno en caso de anomalía de datos */
       FOR UPDATE;
    SET P_ID_DEPOSITO = v_ID_DEPOSITO;
    /* Solo procedemos si encontramos un depósito válido */
    IF v_ID_DEPOSITO IS NOT NULL THEN

        /* 2. Calcular los nuevos valores a guardar */
        /* Sumamos el anticipo actual con el incremento que viene de Delphi */
        SET v_NUEVO_ANTICIPO = v_OLD_ANTICIPO + COALESCE(INC_ANTICIPO, 0);
        
        /* Si Delphi manda NULL (ParamByName('ESTADO').Clear), mantenemos el estado anterior */
        SET v_NUEVO_ESTADO = COALESCE(ESTADO, v_OLD_ESTADO); 

        /* 3. Calcular la variación de la deuda para actualizar al cliente */
        IF v_OLD_ESTADO = 'PENDIENTE' THEN
            SET v_deuda_antigua = (v_OLD_PRECIO * COALESCE(v_OLD_CANTIDAD_PTE, 1)) - v_OLD_ANTICIPO;
        END IF;

        IF v_NUEVO_ESTADO = 'PENDIENTE' THEN
            SET v_deuda_nueva = (v_OLD_PRECIO * COALESCE(v_OLD_CANTIDAD_PTE, 1)) - v_NUEVO_ANTICIPO;
        END IF;

        SET v_diferencia = v_deuda_nueva - v_deuda_antigua;

        /* 4. Actualizar la tabla del depósito */
        UPDATE fza_depositos_cliente
           SET IMPORTE_ANTICIPO_DEP = v_NUEVO_ANTICIPO,
               ESTADO_DEP = v_NUEVO_ESTADO,
               USUARIO_MODIF = USUARIO,
               INSTANTE_MODIF = NOW()
         WHERE ID_DEPOSITO_DEP = v_ID_DEPOSITO;

        /* 5. Actualizar la deuda total en el cliente si hubo cambios */
        IF v_diferencia <> 0 THEN
            UPDATE fza_clientes
               SET TOTAL_DEUDA_CLI = COALESCE(TOTAL_DEUDA_CLI, 0) + v_diferencia
             WHERE CODIGO_CLI_CLI = CLI;
        END IF;

    END IF;

    COMMIT;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_FZA_INVENTARIOS_ACTUALIZAR_TEORICO
DROP PROCEDURE IF EXISTS `PRC_FZA_INVENTARIOS_ACTUALIZAR_TEORICO`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_FZA_INVENTARIOS_ACTUALIZAR_TEORICO`(
    IN p_EMPRESA VARCHAR(10),
    IN p_ALMACEN VARCHAR(10),
    IN p_SERIE   VARCHAR(20),
    IN p_NRO     VARCHAR(20),
    IN p_USUARIO VARCHAR(100)
)
BEGIN
    DECLARE v_DONE INT DEFAULT FALSE;
    DECLARE v_ESTADO VARCHAR(20);
    DECLARE v_FECHA_CABECERA DATETIME;
    DECLARE v_FECHA_DEFECTO  DATETIME;

    DECLARE v_LINEA          VARCHAR(8);
    DECLARE v_SKU            VARCHAR(50);
    DECLARE v_FISICA         DECIMAL(19,6);
    DECLARE v_PMP_NUEVO      DECIMAL(19,6);
    DECLARE v_FECHA_RECUENTO DATETIME;

    DECLARE v_STOCK_HIST     DECIMAL(19,6);
    DECLARE v_PMP_HIST       DECIMAL(19,6);
    DECLARE v_DIF_CANTIDAD   DECIMAL(19,6);
    DECLARE v_TOTAL_COSTE_DIF DECIMAL(19,6);

    DECLARE cur_lineas CURSOR FOR
        SELECT LINEA_INVLIN,
               CODIGO_UNIDAD_INVLIN,
               CANTIDAD_FISICA_INVLIN,
               PRECIO_MEDIO_NUEVO_INVLIN,
               FECHA_RECUENTO_INVLIN
          FROM fza_inventarios_lineas
         WHERE CODIGO_EMP_INVLIN = p_EMPRESA
           AND CODIGO_ALM_INVLIN = p_ALMACEN
           AND SERIE_INV_INVLIN  = p_SERIE
           AND NUMERO_INV_INVLIN = p_NRO;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_DONE = TRUE;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;

    SELECT ESTADO_INV, FECHA_INV
      INTO v_ESTADO, v_FECHA_CABECERA
      FROM fza_inventarios
     WHERE CODIGO_EMP_INV = p_EMPRESA
       AND CODIGO_ALM_INV = p_ALMACEN
       AND SERIE_INV      = p_SERIE
       AND NUMERO_INV     = p_NRO
       FOR UPDATE;

    IF v_ESTADO != 'ABIERTO' THEN
        SIGNAL SQLSTATE '45000'
          SET MESSAGE_TEXT = 'Error: El inventario no esta ABIERTO, no se puede recalcular.';
    END IF;

    SET v_FECHA_DEFECTO = TIMESTAMP(DATE_SUB(DATE(v_FECHA_CABECERA), INTERVAL 1 DAY), '23:59:59');

    OPEN cur_lineas;

    read_loop: LOOP
        FETCH cur_lineas
         INTO v_LINEA, v_SKU, v_FISICA, v_PMP_NUEVO, v_FECHA_RECUENTO;

        IF v_DONE THEN
            LEAVE read_loop;
        END IF;

        IF v_FECHA_RECUENTO IS NULL THEN
            SET v_FECHA_RECUENTO = v_FECHA_DEFECTO;
        END IF;

        SET v_STOCK_HIST = 0;
        SET v_PMP_HIST   = 0;

        BEGIN
            DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN END;
            SELECT IFNULL(SUM(IF(TIPO_MOV = 'E', CANTIDAD_MOV, -CANTIDAD_MOV)), 0)
              INTO v_STOCK_HIST
              FROM fza_movimientos_almacen
             WHERE CODIGO_ALM_MOV    = p_ALMACEN
               AND CODIGO_UNIDAD_MOV = v_SKU
               AND FECHA_MOV        <= v_FECHA_RECUENTO
               AND ESACTIVO_MOV      = 'S';
        END;

        BEGIN
            DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN END;
            SELECT IFNULL(PRECIO_MEDIO_MOV, 0)
              INTO v_PMP_HIST
              FROM fza_movimientos_almacen
             WHERE CODIGO_ALM_MOV    = p_ALMACEN
               AND CODIGO_UNIDAD_MOV = v_SKU
               AND FECHA_MOV        <= v_FECHA_RECUENTO
               AND ESACTIVO_MOV      = 'S'
             ORDER BY FECHA_MOV DESC, NUMERO_MOV DESC
             LIMIT 1;
        END;

        SET v_DIF_CANTIDAD = v_FISICA - v_STOCK_HIST;

        IF v_PMP_NUEVO = 0 OR v_PMP_NUEVO IS NULL THEN
            SET v_PMP_NUEVO = v_PMP_HIST;
        END IF;

        SET v_TOTAL_COSTE_DIF = (v_FISICA * v_PMP_NUEVO) - (v_STOCK_HIST * v_PMP_HIST);

        UPDATE fza_inventarios_lineas
           SET CANTIDAD_TEORICA_INVLIN       = v_STOCK_HIST,
               PRECIO_MEDIO_INVLIN           = v_PMP_HIST,
               PRECIO_MEDIO_NUEVO_INVLIN     = v_PMP_NUEVO,
               CANTIDAD_DIFERENCIA_INVLIN    = v_DIF_CANTIDAD,
               TOTAL_COSTE_DIFERENCIA_INVLIN = v_TOTAL_COSTE_DIF,
               FECHA_RECUENTO_INVLIN         = v_FECHA_RECUENTO,
               USUARIO_MODIF                 = p_USUARIO,
               INSTANTE_MODIF                = NOW()
         WHERE CODIGO_EMP_INVLIN = p_EMPRESA
           AND CODIGO_ALM_INVLIN = p_ALMACEN
           AND SERIE_INV_INVLIN  = p_SERIE
           AND NUMERO_INV_INVLIN = p_NRO
           AND LINEA_INVLIN      = v_LINEA;

    END LOOP;

    CLOSE cur_lineas;

    /* NOTA: ya NO borramos lineas sin diferencia aqui. La purga se hace */
    /* en PRC_FZA_INVENTARIOS_APLICAR, justo antes de generar los movimientos. */

    UPDATE fza_inventarios
       SET TOTAL_UNIDADES_DIFERENCIA_INV = (
               SELECT IFNULL(SUM(CANTIDAD_DIFERENCIA_INVLIN), 0)
                 FROM fza_inventarios_lineas
                WHERE CODIGO_EMP_INVLIN = p_EMPRESA
                  AND CODIGO_ALM_INVLIN = p_ALMACEN
                  AND SERIE_INV_INVLIN  = p_SERIE
                  AND NUMERO_INV_INVLIN = p_NRO
           ),
           TOTAL_EUROS_DIFERENCIA_INV = (
               SELECT IFNULL(SUM(TOTAL_COSTE_DIFERENCIA_INVLIN), 0)
                 FROM fza_inventarios_lineas
                WHERE CODIGO_EMP_INVLIN = p_EMPRESA
                  AND CODIGO_ALM_INVLIN = p_ALMACEN
                  AND SERIE_INV_INVLIN  = p_SERIE
                  AND NUMERO_INV_INVLIN = p_NRO
           ),
           USUARIO_MODIF  = p_USUARIO,
           INSTANTE_MODIF = NOW()
     WHERE CODIGO_EMP_INV = p_EMPRESA
       AND CODIGO_ALM_INV = p_ALMACEN
       AND SERIE_INV      = p_SERIE
       AND NUMERO_INV     = p_NRO;

    COMMIT;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_FZA_INVENTARIOS_APLICAR
DROP PROCEDURE IF EXISTS `PRC_FZA_INVENTARIOS_APLICAR`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_FZA_INVENTARIOS_APLICAR`(
    IN p_EMPRESA VARCHAR(10),
    IN p_ALMACEN VARCHAR(10),
    IN p_SERIE   VARCHAR(20),
    IN p_NRO     VARCHAR(20),
    IN p_USUARIO VARCHAR(100)
)
BEGIN
    DECLARE v_DONE   INT DEFAULT FALSE;
    DECLARE v_ESTADO VARCHAR(20);
    DECLARE v_FECHA_CABECERA DATETIME;
    DECLARE v_FECHA_DEFECTO  DATETIME;

    /* fza_inventarios_lineas.LINEA_INVLIN es VARCHAR(8) (formato
       '00000001'...). El SP original tenia VARCHAR(4) aqui, lo que
       provocaba "#22001 Data too long for column 'v_LINEA'" en el FETCH
       cuando el contador de linea pasaba de 9999. */
    DECLARE v_LINEA     VARCHAR(8);
    DECLARE v_ARTICULO  VARCHAR(20);
    DECLARE v_SKU       VARCHAR(50);
    DECLARE v_TEORICA   DECIMAL(19,6);
    DECLARE v_FISICA    DECIMAL(19,6);
    DECLARE v_PMP_HIST  DECIMAL(19,6);
    DECLARE v_PMP_NUEVO DECIMAL(19,6);
    DECLARE v_FECHA_RECUENTO DATETIME;
    DECLARE v_FECHA_SALIDA   DATETIME;

    DECLARE v_MOV_SALIDA  VARCHAR(20);
    DECLARE v_MOV_ENTRADA VARCHAR(20);

    DECLARE cur_lineas CURSOR FOR
        SELECT l.LINEA_INVLIN,
               l.CODIGO_ART_INVLIN,
               l.CODIGO_UNIDAD_INVLIN,
               l.CANTIDAD_TEORICA_INVLIN,
               l.CANTIDAD_FISICA_INVLIN,
               l.PRECIO_MEDIO_INVLIN,
               l.PRECIO_MEDIO_NUEVO_INVLIN,
               l.FECHA_RECUENTO_INVLIN
          FROM fza_inventarios_lineas l
         WHERE l.CODIGO_EMP_INVLIN = p_EMPRESA
           AND l.CODIGO_ALM_INVLIN = p_ALMACEN
           AND l.SERIE_INV_INVLIN  = p_SERIE
           AND l.NUMERO_INV_INVLIN = p_NRO;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_DONE = TRUE;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    CALL PRC_FZA_INVENTARIOS_ACTUALIZAR_TEORICO(
        p_EMPRESA, p_ALMACEN, p_SERIE, p_NRO, p_USUARIO
    );

    START TRANSACTION;

    SELECT ESTADO_INV, FECHA_INV
      INTO v_ESTADO, v_FECHA_CABECERA
      FROM fza_inventarios
     WHERE CODIGO_EMP_INV = p_EMPRESA
       AND CODIGO_ALM_INV = p_ALMACEN
       AND SERIE_INV      = p_SERIE
       AND NUMERO_INV     = p_NRO
       FOR UPDATE;

    IF v_ESTADO != 'ABIERTO' THEN
        SIGNAL SQLSTATE '45000'
          SET MESSAGE_TEXT = 'Error: El inventario ya fue aplicado o esta cancelado.';
    END IF;

    SET v_FECHA_DEFECTO = TIMESTAMP(DATE_SUB(DATE(v_FECHA_CABECERA), INTERVAL 1 DAY), '23:59:59');

    DELETE FROM fza_inventarios_lineas
     WHERE CODIGO_EMP_INVLIN              = p_EMPRESA
       AND CODIGO_ALM_INVLIN              = p_ALMACEN
       AND SERIE_INV_INVLIN               = p_SERIE
       AND NUMERO_INV_INVLIN              = p_NRO
       AND IFNULL(CANTIDAD_DIFERENCIA_INVLIN,    0) = 0
       AND IFNULL(TOTAL_COSTE_DIFERENCIA_INVLIN, 0) = 0;

    UPDATE fza_inventarios
       SET TOTAL_UNIDADES_DIFERENCIA_INV = (
               SELECT IFNULL(SUM(CANTIDAD_DIFERENCIA_INVLIN), 0)
                 FROM fza_inventarios_lineas
                WHERE CODIGO_EMP_INVLIN = p_EMPRESA
                  AND CODIGO_ALM_INVLIN = p_ALMACEN
                  AND SERIE_INV_INVLIN  = p_SERIE
                  AND NUMERO_INV_INVLIN = p_NRO
           ),
           TOTAL_EUROS_DIFERENCIA_INV = (
               SELECT IFNULL(SUM(TOTAL_COSTE_DIFERENCIA_INVLIN), 0)
                 FROM fza_inventarios_lineas
                WHERE CODIGO_EMP_INVLIN = p_EMPRESA
                  AND CODIGO_ALM_INVLIN = p_ALMACEN
                  AND SERIE_INV_INVLIN  = p_SERIE
                  AND NUMERO_INV_INVLIN = p_NRO
           )
     WHERE CODIGO_EMP_INV = p_EMPRESA
       AND CODIGO_ALM_INV = p_ALMACEN
       AND SERIE_INV      = p_SERIE
       AND NUMERO_INV     = p_NRO;

    /* Temp para recolectar los SKUs tocados durante la generacion de movs.
       Se rellena dentro del bucle y se consume al final con una sola
       llamada al recalculo en lote. */
    DROP TEMPORARY TABLE IF EXISTS tmp_skus_recalc;
    CREATE TEMPORARY TABLE tmp_skus_recalc (
        sku VARCHAR(50) NOT NULL PRIMARY KEY
    ) ENGINE=InnoDB;

    OPEN cur_lineas;

    read_loop: LOOP
        FETCH cur_lineas
         INTO v_LINEA, v_ARTICULO, v_SKU, v_TEORICA, v_FISICA, v_PMP_HIST, v_PMP_NUEVO, v_FECHA_RECUENTO;

        IF v_DONE THEN
            LEAVE read_loop;
        END IF;

        IF v_FECHA_RECUENTO IS NULL THEN
            SET v_FECHA_RECUENTO = v_FECHA_DEFECTO;
        END IF;

        SET v_FECHA_SALIDA = DATE_SUB(v_FECHA_RECUENTO, INTERVAL 1 SECOND);
        SET v_MOV_SALIDA  = LEFT(CONCAT('IV-', p_NRO, '-', v_LINEA, 'S'), 20);
        SET v_MOV_ENTRADA = LEFT(CONCAT('IV-', p_NRO, '-', v_LINEA, 'E'), 20);

        IF v_TEORICA <> 0 THEN
            CALL PRC_FZA_MOVIMIENTOS_ALMACEN_INSERT(
                v_MOV_SALIDA, 'IN', p_SERIE, p_NRO, v_LINEA,
                p_EMPRESA, p_ALMACEN, NULL, v_SKU,
                'S', v_TEORICA, v_PMP_HIST, (v_TEORICA * v_PMP_HIST),
                p_USUARIO, p_ALMACEN, NULL, NULL, NULL, v_ARTICULO
            );
            UPDATE fza_movimientos_almacen
               SET FECHA_MOV = v_FECHA_SALIDA
             WHERE NUMERO_MOV = v_MOV_SALIDA;
        END IF;

        IF v_FISICA > 0 THEN
            CALL PRC_FZA_MOVIMIENTOS_ALMACEN_INSERT(
                v_MOV_ENTRADA, 'IN', p_SERIE, p_NRO, v_LINEA,
                p_EMPRESA, p_ALMACEN, NULL, v_SKU,
                'E', v_FISICA, v_PMP_NUEVO, (v_FISICA * v_PMP_NUEVO),
                p_USUARIO, p_ALMACEN, NULL, NULL, NULL, v_ARTICULO
            );
            UPDATE fza_movimientos_almacen
               SET FECHA_MOV = v_FECHA_RECUENTO
             WHERE NUMERO_MOV = v_MOV_ENTRADA;
        END IF;

        /* Anotamos el SKU para el recalculo en lote. INSERT IGNORE evita
           duplicados si varias lineas tocan el mismo SKU. */
        INSERT IGNORE INTO tmp_skus_recalc (sku) VALUES (v_SKU);

    END LOOP;

    CLOSE cur_lineas;

    /* Recalculo set-based sobre todos los SKUs tocados. Reemplaza la
       llamada SP_RECALCULAR_PMP_SKU_ALMACEN por linea del cursor. */
    CALL SP_RECALCULAR_PMP_LOTE_ALMACEN(p_EMPRESA, p_ALMACEN);

    UPDATE fza_inventarios
       SET ESTADO_INV     = 'APLICADO',
           USUARIO_MODIF  = p_USUARIO,
           INSTANTE_MODIF = NOW()
     WHERE CODIGO_EMP_INV = p_EMPRESA
       AND CODIGO_ALM_INV = p_ALMACEN
       AND SERIE_INV      = p_SERIE
       AND NUMERO_INV     = p_NRO;

    DROP TEMPORARY TABLE IF EXISTS tmp_skus_recalc;

    COMMIT;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_FZA_INVENTARIOS_ELIMINAR_REGUL
DROP PROCEDURE IF EXISTS `PRC_FZA_INVENTARIOS_ELIMINAR_REGUL`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_FZA_INVENTARIOS_ELIMINAR_REGUL`(
    IN p_EMPRESA VARCHAR(10),
    IN p_ALMACEN VARCHAR(10),
    IN p_SERIE   VARCHAR(20),
    IN p_NRO     VARCHAR(20),
    IN p_USUARIO VARCHAR(100)
)
BEGIN
    DECLARE v_ESTADO VARCHAR(20);
    DECLARE v_PATRON VARCHAR(50);

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;

    /* 1. Verificar estado del inventario (debe estar APLICADO). */
    SELECT ESTADO_INV INTO v_ESTADO
      FROM fza_inventarios
     WHERE CODIGO_EMP_INV = p_EMPRESA
       AND CODIGO_ALM_INV = p_ALMACEN
       AND SERIE_INV      = p_SERIE
       AND NUMERO_INV     = p_NRO
       FOR UPDATE;

    IF v_ESTADO IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT =
            'Error: el inventario no existe.';
    END IF;

    IF v_ESTADO <> 'APLICADO' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT =
            'Error: el inventario debe estar APLICADO para eliminar la regularizacion.';
    END IF;

    SET v_PATRON = CONCAT('IV-', p_NRO, '-%');

    /* 2. Recoger en una temp los SKUs afectados ANTES de borrar. La temp
       sirve de input a SP_RECALCULAR_PMP_LOTE_ALMACEN. */
    DROP TEMPORARY TABLE IF EXISTS tmp_skus_recalc;
    CREATE TEMPORARY TABLE tmp_skus_recalc (
        sku VARCHAR(50) NOT NULL PRIMARY KEY
    ) ENGINE=InnoDB;

    INSERT INTO tmp_skus_recalc (sku)
    SELECT DISTINCT CODIGO_UNIDAD_MOV
      FROM fza_movimientos_almacen
     WHERE CODIGO_ALM_MOV = p_ALMACEN
       AND NUMERO_MOV LIKE v_PATRON;

    /* 3. Borrar los movimientos generados por el inventario. NUMERO_MOV es
       la PK; el LIKE con prefijo constante permite al optimizador usar la
       PK directamente. El AND CODIGO_ALM_MOV se mantiene por seguridad (un
       inventario solo genera movs en su almacen). */
    DELETE FROM fza_movimientos_almacen
     WHERE CODIGO_ALM_MOV = p_ALMACEN
       AND NUMERO_MOV LIKE v_PATRON;

    /* 4. Recalcular PMP y stockactual de todos los SKUs afectados en
       UNA sola pasada. */
    CALL SP_RECALCULAR_PMP_LOTE_ALMACEN(p_EMPRESA, p_ALMACEN);

    /* 5. Marcar el inventario como ABIERTO de nuevo. */
    UPDATE fza_inventarios
       SET ESTADO_INV     = 'ABIERTO',
           USUARIO_MODIF  = p_USUARIO,
           INSTANTE_MODIF = NOW()
     WHERE CODIGO_EMP_INV = p_EMPRESA
       AND CODIGO_ALM_INV = p_ALMACEN
       AND SERIE_INV      = p_SERIE
       AND NUMERO_INV     = p_NRO;

    DROP TEMPORARY TABLE IF EXISTS tmp_skus_recalc;

    COMMIT;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_FZA_MOVIMIENTOS_ALMACEN_DELETE
DROP PROCEDURE IF EXISTS `PRC_FZA_MOVIMIENTOS_ALMACEN_DELETE`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_FZA_MOVIMIENTOS_ALMACEN_DELETE`(
    IN `p_NUMERO_MOV` VARCHAR(20)
)
BEGIN
    DECLARE v_TIPO_DOC VARCHAR(20);
    DECLARE v_TIPO_MOV VARCHAR(1);
    DECLARE v_ALM VARCHAR(10);
    DECLARE v_UNI VARCHAR(50);
    DECLARE v_CANT DECIMAL(19,6);
    DECLARE v_VALOR DECIMAL(19,6);
    SELECT TIPO_DOC_MOV, TIPO_MOV, CODIGO_ALM_MOV, CODIGO_UNIDAD_MOV,
           CANTIDAD_MOV, TOTAL_COSTE_MOV
      INTO v_TIPO_DOC, v_TIPO_MOV, v_ALM, v_UNI, v_CANT, v_VALOR
      FROM fza_movimientos_almacen
     WHERE NUMERO_MOV = p_NUMERO_MOV
       AND ESACTIVO_MOV = 'S'
     LIMIT 1;
    IF v_UNI IS NOT NULL THEN
        CALL PRC_FZA_AJUSTAR_ACUMULADO_STK(
            v_TIPO_DOC, v_TIPO_MOV, v_ALM, v_UNI, v_CANT, v_VALOR, -1);
        DELETE FROM fza_movimientos_almacen WHERE NUMERO_MOV = p_NUMERO_MOV;
    END IF;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_FZA_MOVIMIENTOS_ALMACEN_DELETE_DOC
DROP PROCEDURE IF EXISTS `PRC_FZA_MOVIMIENTOS_ALMACEN_DELETE_DOC`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_FZA_MOVIMIENTOS_ALMACEN_DELETE_DOC`(
    IN `p_TIPO_DOC_MOV` VARCHAR(20),
    IN `p_SERIE_DOC_MOV` VARCHAR(20),
    IN `p_NRO_DOC_MOV` VARCHAR(20)
)
BEGIN
    DECLARE v_done INT DEFAULT 0;
    DECLARE v_NUM VARCHAR(20);
    DECLARE cur CURSOR FOR
        SELECT NUMERO_MOV
          FROM fza_movimientos_almacen
         WHERE TIPO_DOC_MOV   = p_TIPO_DOC_MOV
           AND SERIE_DOC_MOV  = p_SERIE_DOC_MOV
           AND NUMERO_DOC_MOV = p_NRO_DOC_MOV
           AND ESACTIVO_MOV   = 'S';
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;
    OPEN cur;
    bucle: LOOP
        FETCH cur INTO v_NUM;
        IF v_done = 1 THEN
            LEAVE bucle;
        END IF;
        CALL PRC_FZA_MOVIMIENTOS_ALMACEN_DELETE(v_NUM);
    END LOOP;
    CLOSE cur;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_FZA_MOVIMIENTOS_ALMACEN_INSERT
DROP PROCEDURE IF EXISTS `PRC_FZA_MOVIMIENTOS_ALMACEN_INSERT`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_FZA_MOVIMIENTOS_ALMACEN_INSERT`(
    IN `p_NUMERO_MOV` VARCHAR(20),
    IN `p_TIPO_DOC_MOV` VARCHAR(20),
    IN `p_SERIE_DOC_MOV` VARCHAR(20),
    IN `p_NRO_DOC_MOV` VARCHAR(20),
    IN `p_LINEA_MOV` VARCHAR(10),
    IN `p_CODIGO_EMPRESA_MOV` VARCHAR(20),
    IN `p_CODIGO_ALMACEN_MOV` VARCHAR(10),
    IN `p_CODIGO_ALMACEN_CONTRA_MOV` VARCHAR(10),
    IN `p_CODIGO_UNIDAD_MOV` VARCHAR(50),
    IN `p_TIPO_MOVIMIENTO_MOV` VARCHAR(1),
    IN `p_CANTIDAD_MOV` DECIMAL(19,6),
    IN `p_PRECIO_MEDIO_MOV` DECIMAL(19,6),
    IN `p_TOTAL_COSTE_MOV` DECIMAL(19,6),
    IN `p_USUARIO` VARCHAR(100),
    IN `p_ALMACEN_DOC` VARCHAR(10),
    IN `p_NUMOP_DOC` VARCHAR(20),
    IN `p_CODIGO_CAJA_DOC_MOV` VARCHAR(10),
    IN `p_CODCLIENTE` VARCHAR(20),
    IN `p_CODARTICULO` VARCHAR(20)
)
BEGIN
    DECLARE v_PMPActual DECIMAL(19,6) DEFAULT 0;
    DECLARE v_PrecioFinal DECIMAL(19,6);
    DECLARE v_CosteFinal DECIMAL(19,6);
    DECLARE v_dEntCompra DECIMAL(19,6) DEFAULT 0;
    DECLARE v_dEntTraspaso, v_dSalTraspaso DECIMAL(19,6) DEFAULT 0;
    DECLARE v_dEntDeposito, v_dSalDeposito DECIMAL(19,6) DEFAULT 0;
    DECLARE v_dSalVenta DECIMAL(19,6) DEFAULT 0;
    DECLARE v_dEntRegular DECIMAL(19,6) DEFAULT 0;
    DECLARE v_dSalAlbVenta DECIMAL(19,6) DEFAULT 0;
    DECLARE v_dEntAlbEntrada DECIMAL(19,6) DEFAULT 0;

    SELECT IFNULL(PRECIO_MEDIO_STK, 0)
      INTO v_PMPActual
      FROM fza_articulos_stockactual
     WHERE CODIGO_ALM_STK = p_CODIGO_ALMACEN_MOV
       AND CODIGO_UNIDAD_STK = p_CODIGO_UNIDAD_MOV
     LIMIT 1;

    IF p_TIPO_MOVIMIENTO_MOV = 'S' THEN
        SET v_PrecioFinal = v_PMPActual;
        SET v_CosteFinal  = p_CANTIDAD_MOV * v_PMPActual;
    ELSE
        SET v_PrecioFinal = p_PRECIO_MEDIO_MOV;
        SET v_CosteFinal  = p_TOTAL_COSTE_MOV;
    END IF;

    /* Delta de acumulado según TIPO_DOC_MOV */
    IF p_TIPO_DOC_MOV = 'AC' AND p_TIPO_MOVIMIENTO_MOV = 'E' THEN
        SET v_dEntCompra = p_CANTIDAD_MOV;
    ELSEIF p_TIPO_DOC_MOV IN ('TR','AT') THEN
        IF p_TIPO_MOVIMIENTO_MOV = 'E' THEN SET v_dEntTraspaso = p_CANTIDAD_MOV;
        ELSE SET v_dSalTraspaso = p_CANTIDAD_MOV; END IF;
    ELSEIF p_TIPO_DOC_MOV = 'DP' THEN
        IF p_TIPO_MOVIMIENTO_MOV = 'E' THEN SET v_dEntDeposito = p_CANTIDAD_MOV;
        ELSE SET v_dSalDeposito = p_CANTIDAD_MOV; END IF;
    ELSEIF p_TIPO_DOC_MOV = 'VE' AND p_TIPO_MOVIMIENTO_MOV = 'S' THEN
        SET v_dSalVenta = p_CANTIDAD_MOV;
    ELSEIF p_TIPO_DOC_MOV = 'IN' AND p_TIPO_MOVIMIENTO_MOV = 'E' THEN
        SET v_dEntRegular = p_CANTIDAD_MOV;
    ELSEIF p_TIPO_DOC_MOV = 'AV' AND p_TIPO_MOVIMIENTO_MOV = 'S' THEN
        SET v_dSalAlbVenta = p_CANTIDAD_MOV;
    ELSEIF p_TIPO_DOC_MOV = 'AE' AND p_TIPO_MOVIMIENTO_MOV = 'E' THEN
        SET v_dEntAlbEntrada = p_CANTIDAD_MOV;
    END IF;

    INSERT INTO fza_movimientos_almacen (
        NUMERO_MOV,
        TIPO_DOC_MOV, SERIE_DOC_MOV, NUMERO_DOC_MOV, LINEA_MOV,
        CODIGO_EMP_MOV, CODIGO_ALM_MOV, CODIGO_ALM_CONTRA_MOV,
        CODIGO_UNIDAD_MOV, TIPO_MOV, CANTIDAD_MOV,
        PRECIO_COSTE_UNITARIO_MOV,
        PRECIO_MEDIO_MOV, TOTAL_COSTE_MOV,
        FECHA_MOV, USUARIO_ALTA, USUARIO_MODIF,
        CODIGO_ALM_DOC_MOV, NUMERO_OPERACION_DOC_MOV, CODIGO_CAJA_DOC_MOV,
        CODIGO_CLI_MOV, CODIGO_ART_MOV
    ) VALUES (
        p_NUMERO_MOV,
        p_TIPO_DOC_MOV, p_SERIE_DOC_MOV, p_NRO_DOC_MOV, p_LINEA_MOV,
        p_CODIGO_EMPRESA_MOV, p_CODIGO_ALMACEN_MOV, p_CODIGO_ALMACEN_CONTRA_MOV,
        p_CODIGO_UNIDAD_MOV, p_TIPO_MOVIMIENTO_MOV, p_CANTIDAD_MOV,
        v_PrecioFinal,
        v_PrecioFinal, v_CosteFinal,
        NOW(), p_USUARIO, p_USUARIO,
        p_ALMACEN_DOC, p_NUMOP_DOC, p_CODIGO_CAJA_DOC_MOV,
        p_CODCLIENTE, p_CODARTICULO
    );

    INSERT INTO fza_articulos_stockactual (
        CODIGO_ALM_STK, CODIGO_UNIDAD_STK,
        CANTIDAD_STK, VALOR_TOTAL_STK, PRECIO_MEDIO_STK, INSTANTE_MODIF,
        CANTIDAD_ENT_COMPRA_STK,
        CANTIDAD_ENT_TRASPASO_STK, CANTIDAD_SAL_TRASPASO_STK,
        CANTIDAD_ENT_DEPOSITO_STK, CANTIDAD_SAL_DEPOSITO_STK,
        CANTIDAD_SAL_VENTA_STK,
        CANTIDAD_ENT_REGULAR_STK,
        CANTIDAD_SAL_ALBVENTA_STK,
        CANTIDAD_ENT_ALBENTRADA_STK
    ) VALUES (
        p_CODIGO_ALMACEN_MOV,
        p_CODIGO_UNIDAD_MOV,
        IF(p_TIPO_MOVIMIENTO_MOV = 'E', p_CANTIDAD_MOV, -p_CANTIDAD_MOV),
        IF(p_TIPO_MOVIMIENTO_MOV = 'E', v_CosteFinal, -v_CosteFinal),
        v_PrecioFinal,
        NOW(),
        v_dEntCompra,
        v_dEntTraspaso, v_dSalTraspaso,
        v_dEntDeposito, v_dSalDeposito,
        v_dSalVenta,
        v_dEntRegular,
        v_dSalAlbVenta,
        v_dEntAlbEntrada
    )
    ON DUPLICATE KEY UPDATE
        CANTIDAD_STK     = CANTIDAD_STK     + VALUES(CANTIDAD_STK),
        VALOR_TOTAL_STK  = VALOR_TOTAL_STK  + VALUES(VALOR_TOTAL_STK),
        PRECIO_MEDIO_STK = IF(CANTIDAD_STK > 0,
                              VALOR_TOTAL_STK / CANTIDAD_STK, 0),
        INSTANTE_MODIF   = NOW(),
        CANTIDAD_ENT_COMPRA_STK   = CANTIDAD_ENT_COMPRA_STK   + VALUES(CANTIDAD_ENT_COMPRA_STK),
        CANTIDAD_ENT_TRASPASO_STK = CANTIDAD_ENT_TRASPASO_STK + VALUES(CANTIDAD_ENT_TRASPASO_STK),
        CANTIDAD_SAL_TRASPASO_STK = CANTIDAD_SAL_TRASPASO_STK + VALUES(CANTIDAD_SAL_TRASPASO_STK),
        CANTIDAD_ENT_DEPOSITO_STK = CANTIDAD_ENT_DEPOSITO_STK + VALUES(CANTIDAD_ENT_DEPOSITO_STK),
        CANTIDAD_SAL_DEPOSITO_STK = CANTIDAD_SAL_DEPOSITO_STK + VALUES(CANTIDAD_SAL_DEPOSITO_STK),
        CANTIDAD_SAL_VENTA_STK    = CANTIDAD_SAL_VENTA_STK    + VALUES(CANTIDAD_SAL_VENTA_STK),
        CANTIDAD_ENT_REGULAR_STK  = CANTIDAD_ENT_REGULAR_STK  + VALUES(CANTIDAD_ENT_REGULAR_STK),
        CANTIDAD_SAL_ALBVENTA_STK   = CANTIDAD_SAL_ALBVENTA_STK   + VALUES(CANTIDAD_SAL_ALBVENTA_STK),
        CANTIDAD_ENT_ALBENTRADA_STK = CANTIDAD_ENT_ALBENTRADA_STK + VALUES(CANTIDAD_ENT_ALBENTRADA_STK);
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_FZA_MOVIMIENTOS_ALMACEN_UPDATE
DROP PROCEDURE IF EXISTS `PRC_FZA_MOVIMIENTOS_ALMACEN_UPDATE`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_FZA_MOVIMIENTOS_ALMACEN_UPDATE`(
    IN `p_NUMERO_MOV` VARCHAR(20),
    IN `p_NUEVA_CANTIDAD` DECIMAL(19,6),
    IN `p_NUEVO_PRECIO` DECIMAL(19,6),
    IN `p_USUARIO` VARCHAR(100)
)
BEGIN
    DECLARE v_TIPO_DOC VARCHAR(20);
    DECLARE v_TIPO_MOV VARCHAR(1);
    DECLARE v_ALM VARCHAR(10);
    DECLARE v_UNI VARCHAR(50);
    DECLARE v_CANT_ANT DECIMAL(19,6);
    DECLARE v_VALOR_ANT DECIMAL(19,6);
    DECLARE v_VALOR_NUEVO DECIMAL(19,6);
    SELECT TIPO_DOC_MOV, TIPO_MOV, CODIGO_ALM_MOV, CODIGO_UNIDAD_MOV,
           CANTIDAD_MOV, TOTAL_COSTE_MOV
      INTO v_TIPO_DOC, v_TIPO_MOV, v_ALM, v_UNI, v_CANT_ANT, v_VALOR_ANT
      FROM fza_movimientos_almacen
     WHERE NUMERO_MOV = p_NUMERO_MOV
       AND ESACTIVO_MOV = 'S'
     LIMIT 1;
    IF v_UNI IS NOT NULL THEN
        SET v_VALOR_NUEVO = p_NUEVA_CANTIDAD * p_NUEVO_PRECIO;
        /* Revertir el movimiento original del stock */
        CALL PRC_FZA_AJUSTAR_ACUMULADO_STK(
            v_TIPO_DOC, v_TIPO_MOV, v_ALM, v_UNI, v_CANT_ANT, v_VALOR_ANT, -1);
        /* Aplicar el nuevo */
        CALL PRC_FZA_AJUSTAR_ACUMULADO_STK(
            v_TIPO_DOC, v_TIPO_MOV, v_ALM, v_UNI, p_NUEVA_CANTIDAD, v_VALOR_NUEVO, +1);
        /* Actualizar el registro del movimiento */
        UPDATE fza_movimientos_almacen
           SET CANTIDAD_MOV = p_NUEVA_CANTIDAD,
               PRECIO_COSTE_UNITARIO_MOV = p_NUEVO_PRECIO,
               PRECIO_MEDIO_MOV          = p_NUEVO_PRECIO,
               TOTAL_COSTE_MOV           = v_VALOR_NUEVO,
               USUARIO_MODIF             = p_USUARIO,
               INSTANTE_MODIF            = NOW()
         WHERE NUMERO_MOV = p_NUMERO_MOV;
    END IF;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_GENERAR_CODIGO_VALE
DROP PROCEDURE IF EXISTS `PRC_GENERAR_CODIGO_VALE`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_GENERAR_CODIGO_VALE`(IN pEmpresa VARCHAR(10),
    IN pAlmacen VARCHAR(10),
    IN pCaja VARCHAR(10),
    IN pNumOperacion VARCHAR(20),
    IN pUsuario VARCHAR(100),
    OUT pCodigoFinal VARCHAR(100))
BEGIN
    /* DECLARE vContador VARCHAR(20); */
    
    /* 1. Obtenemos el contador secuencial (Le pasamos 'VL' y el Usuario) */
    /* CALL PRC_GET_NEXT_CONT('VL', pUsuario, vContador); */
    
    /* 2. Construimos el código final concatenando todo */
    /* Resultado ejemplo: 00014_VL_1_ALM01_CAJ1_4509 */
    SET pCodigoFinal = CONCAT(pEmpresa, '_', pAlmacen, '_', pCaja, '_', pNumOperacion);
    
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_GETPERFILFORMULARIO
DROP PROCEDURE IF EXISTS `PRC_GETPERFILFORMULARIO`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_GETPERFILFORMULARIO`(IN `p_usuario` VARCHAR(200),
    IN `p_grupo` VARCHAR(200),
    IN `p_formulario` VARCHAR(100))
BEGIN
    /* Usamos una CTE para calcular la prioridad y luego filtrar */
    WITH perfiles_con_prioridad AS (
        SELECT 
            USUARIO_GRUPO_USUPER,
            KEY_USUPER,
            SUBKEY_USUPER,
            VALUE_USUPER,
            VALUE_TEXT_USUPER,
            TYPE_BLOB_USUPER,
            VALUE_BLOB_USUPER,
            CASE USUARIO_GRUPO_USUPER
                WHEN p_usuario THEN 1
                WHEN p_grupo   THEN 2
                WHEN 'Todos'   THEN 3
            END AS prioridad,
            ROW_NUMBER() OVER (
                PARTITION BY SUBKEY_USUPER 
                ORDER BY CASE USUARIO_GRUPO_USUPER
                    WHEN p_usuario THEN 1
                    WHEN p_grupo   THEN 2
                    WHEN 'Todos'   THEN 3
                END
            ) AS rn
        FROM fza_usuarios_perfiles
        WHERE KEY_USUPER = p_formulario 
          AND USUARIO_GRUPO_USUPER IN (p_usuario, p_grupo, 'Todos')
    )
    SELECT 
        USUARIO_GRUPO_USUPER,
        KEY_USUPER,
        SUBKEY_USUPER,
        VALUE_USUPER,
        VALUE_TEXT_USUPER,
        TYPE_BLOB_USUPER,
        VALUE_BLOB_USUPER
    FROM perfiles_con_prioridad
    WHERE rn = 1
    ORDER BY SUBKEY_USUPER;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_GET_BALANCE_ALMACEN_SIN_TALLAS
DROP PROCEDURE IF EXISTS `PRC_GET_BALANCE_ALMACEN_SIN_TALLAS`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_GET_BALANCE_ALMACEN_SIN_TALLAS`(
    IN `p_MODO`         VARCHAR(1),   /* 'F' entre fechas, 'A' por acumulados */
    IN `p_DESDE`        DATE,         /* inclusive (solo modo 'F') */
    IN `p_HASTA`        DATE,         /* inclusive (solo modo 'F') */
    IN `p_ALMACENES`    TEXT,         /* CSV "01,50" o '' = todos los activos */
    IN `p_FAMILIAS`     TEXT,         /* CSV; '' = todas. Una padre incluye sus hijas */
    IN `p_PROVEEDORES`  TEXT,         /* CSV de códigos de proveedor; '' = todos */
    IN `p_TEMPORADAS`   TEXT,         /* CSV de valores de temporada; '' = todas */
    IN `p_COD_TARIFA`   VARCHAR(20),  /* tarifa para valorar ventas/salidas */
    IN `p_DESGLOSADO`   VARCHAR(1),   /* 'S'/'N' (solo aplica a modo 'F') */
    IN `p_BANDAS`       TEXT,         /* CSV de códigos de banda; '' = todas */
    IN `p_NIVEL1`       VARCHAR(3),   /* 1er nivel de agrupación: PRV/FAM/TMP/ALM/'' */
    IN `p_NIVEL2`       VARCHAR(3),   /* 2o nivel de agrupación */
    IN `p_NIVEL3`       VARCHAR(3),   /* 3er nivel de agrupación */
    IN `p_NIVEL_FAM`    INT           /* nivel del árbol de familias al agrupar */
)
BEGIN
    DECLARE v_alms      TEXT;
    DECLARE v_tarifa    VARCHAR(20);
    DECLARE v_desde     DATE;
    DECLARE v_hasta     DATE;
    DECLARE v_por_alm   BOOLEAN DEFAULT FALSE;  /* TRUE si se agrupa por almacén */
    DECLARE v_nivel_fam INT;                    /* nivel efectivo del árbol fam. */
    /* Normalización de parámetros. */
    SET p_MODO       = IFNULL(NULLIF(p_MODO, ''), 'A');
    SET p_DESGLOSADO  = IFNULL(NULLIF(p_DESGLOSADO, ''), 'N');
    SET p_FAMILIAS    = IFNULL(p_FAMILIAS, '');
    SET p_PROVEEDORES = IFNULL(p_PROVEEDORES, '');
    SET p_TEMPORADAS  = IFNULL(p_TEMPORADAS, '');
    SET p_BANDAS      = IFNULL(p_BANDAS, '');
    /* Niveles de agrupación: normalizados a mayúsculas. Se admiten PRV */
    /* (proveedor), FAM (familia), TMP (temporada) y ALM (almacén); cualquier */
    /* otro valor (o vacío) deshabilita ese nivel. */
    SET p_NIVEL1      = UPPER(IFNULL(p_NIVEL1, ''));
    SET p_NIVEL2      = UPPER(IFNULL(p_NIVEL2, ''));
    SET p_NIVEL3      = UPPER(IFNULL(p_NIVEL3, ''));
    SET v_por_alm     = (p_NIVEL1 = 'ALM' OR p_NIVEL2 = 'ALM' OR p_NIVEL3 = 'ALM');
    SET v_nivel_fam   = IF(IFNULL(p_NIVEL_FAM, 0) < 1, 9999, p_NIVEL_FAM);
    SET v_tarifa      = IFNULL(NULLIF(p_COD_TARIFA, ''), 'PVP');
    SET v_desde      = IFNULL(p_DESDE, '1900-01-01');
    SET v_hasta      = IFNULL(p_HASTA, CURRENT_DATE);
    /* Lista efectiva de almacenes (CSV sin comillas, para FIND_IN_SET). */
    IF IFNULL(p_ALMACENES, '') <> '' THEN
        SET v_alms = p_ALMACENES;
    ELSE
        SELECT GROUP_CONCAT(`CODIGO_ALM_ALM`)
          INTO v_alms
          FROM `fza_almacenes`
         WHERE `ESACTIVO_ALM` = 'S';
    END IF;
    SET v_alms = IFNULL(v_alms, '');

    /* ----------------------------------------------------------------- */
    /* Filtros de artículo (familias con descendencia, proveedores, */
    /* temporadas) + mapa de familia por nivel del árbol. Igual que el */
    /* balance por tallas. */
    /* ----------------------------------------------------------------- */
    DROP TEMPORARY TABLE IF EXISTS `tmp_bst_fam`;
    CREATE TEMPORARY TABLE `tmp_bst_fam` (
        `CODIGO_FAM` VARCHAR(20) NOT NULL PRIMARY KEY
    );
    INSERT IGNORE INTO `tmp_bst_fam` (`CODIGO_FAM`)
    WITH RECURSIVE `fam_tree` AS (
        SELECT `CODIGO_FAM_FAM`
          FROM `fza_articulos_familias`
         WHERE FIND_IN_SET(`CODIGO_FAM_FAM`, p_FAMILIAS)
        UNION ALL
        SELECT f.`CODIGO_FAM_FAM`
          FROM `fza_articulos_familias` f
          JOIN `fam_tree` t ON f.`CODIGO_PADRE_FAM` = t.`CODIGO_FAM_FAM`
    )
    SELECT DISTINCT `CODIGO_FAM_FAM` FROM `fam_tree`;
    /* Mapa familia -> ancestro al nivel pedido (para agrupar por FAM "por */
    /* nivel"). Ver balance_almacen_tallas.sql para el detalle. */
    DROP TEMPORARY TABLE IF EXISTS `tmp_bst_fam_grp`;
    CREATE TEMPORARY TABLE `tmp_bst_fam_grp` (
        `CODIGO_FAM` VARCHAR(20)  NOT NULL PRIMARY KEY,
        `COD_GRP`    VARCHAR(20)  NOT NULL,
        `DESC_GRP`   VARCHAR(200) NULL
    );
    INSERT IGNORE INTO `tmp_bst_fam_grp` (`CODIGO_FAM`, `COD_GRP`, `DESC_GRP`)
    WITH RECURSIVE `fam_path` AS (
        SELECT `CODIGO_FAM_FAM` AS `COD`,
               CAST(`CODIGO_FAM_FAM` AS CHAR(1000)) AS `RUTA`
          FROM `fza_articulos_familias`
         WHERE `CODIGO_PADRE_FAM` IS NULL OR `CODIGO_PADRE_FAM` = ''
        UNION ALL
        SELECT f.`CODIGO_FAM_FAM`,
               CONCAT(pa.`RUTA`, '>', f.`CODIGO_FAM_FAM`)
          FROM `fza_articulos_familias` f
          JOIN `fam_path` pa ON f.`CODIGO_PADRE_FAM` = pa.`COD`
    )
    SELECT pa.`COD`,
           SUBSTRING_INDEX(SUBSTRING_INDEX(pa.`RUTA`, '>', v_nivel_fam), '>', -1),
           NULL
      FROM `fam_path` pa;
    UPDATE `tmp_bst_fam_grp` g
      JOIN `fza_articulos_familias` f ON f.`CODIGO_FAM_FAM` = g.`COD_GRP`
       SET g.`DESC_GRP` = COALESCE(f.`DESCRIPCION_FAM`, f.`NOMBRE_FAM_FAM`,
                                   g.`COD_GRP`);
    /* Conjunto de artículos activos que pasan familia, proveedor y temporada. */
    DROP TEMPORARY TABLE IF EXISTS `tmp_bst_arts`;
    CREATE TEMPORARY TABLE `tmp_bst_arts` (
        `CODIGO_ART` VARCHAR(20) NOT NULL PRIMARY KEY
    );
    INSERT IGNORE INTO `tmp_bst_arts` (`CODIGO_ART`)
    SELECT a.`CODIGO_ART_ART`
      FROM `fza_articulos` a
     WHERE a.`ESACTIVO_ART` = 'S'
       AND (p_FAMILIAS = ''
            OR a.`CODIGO_FAM_ART` IN (SELECT `CODIGO_FAM` FROM `tmp_bst_fam`))
       AND (p_PROVEEDORES = ''
            OR EXISTS (SELECT 1 FROM `fza_articulos_proveedores` ap
                        WHERE ap.`CODIGO_ART_AP` = a.`CODIGO_ART_ART`
                          AND FIND_IN_SET(ap.`CODIGO_PRV_AP`, p_PROVEEDORES)))
       AND (p_TEMPORADAS = ''
            OR EXISTS (SELECT 1 FROM `fza_articulos_propiedades` tp
                        LEFT JOIN `fza_propiedades_valores` tpv
                          ON tpv.`ID_PV_ARTPROP` = tp.`ID_PV_ARTPROP`
                        WHERE tp.`CODIGO_ART_ART` = a.`CODIGO_ART_ART`
                          AND tp.`CODIGO_PROP_ARTPROP` = 'TEMPORADA'
                          AND FIND_IN_SET(
                                COALESCE(tpv.`PV`, tp.`VALOR_LIBRE_ARTPROP`),
                                p_TEMPORADAS)));

    /* ----------------------------------------------------------------- */
    /* SKUs en juego: unidad -> (artículo, color). SIN posición de talla: */
    /* entra cualquier SKU de los artículos filtrados (con o sin tallas), */
    /* por eso el informe cubre todo el catálogo. */
    /* ----------------------------------------------------------------- */
    DROP TEMPORARY TABLE IF EXISTS `tmp_bst_sku`;
    CREATE TEMPORARY TABLE `tmp_bst_sku` (
        `CODIGO_UNIDAD` VARCHAR(50)  NOT NULL PRIMARY KEY,
        `CODIGO_ART`    VARCHAR(20)  NOT NULL,
        `COLOR`         VARCHAR(100) NOT NULL DEFAULT '',
        `COLOR_HEX`     VARCHAR(7)   NULL,
        `ORDEN_COLOR`   INT          NOT NULL DEFAULT 0,
        KEY `IDX_BST_SKU_ART` (`CODIGO_ART`)
    );
    INSERT IGNORE INTO `tmp_bst_sku`
    SELECT sku.`CODIGO_UNIDAD_SKU`, sku.`CODIGO_ART_SKU`,
           COALESCE(co.`AV`, ''), COALESCE(atb.`HEX_ATB`, ''),
           COALESCE(co.`ORDEN_AV`, 0)
      FROM `fza_articulos_skus` sku
      JOIN `fza_articulos` a
        ON a.`CODIGO_ART_ART` = sku.`CODIGO_ART_SKU`
       AND a.`ESACTIVO_ART` = 'S'
       AND a.`CODIGO_ART_ART` IN (SELECT `CODIGO_ART` FROM `tmp_bst_arts`)
      LEFT JOIN `fza_atributos_sku` sac
        ON sac.`CODIGO_UNIDAD_SKU_SA` = sku.`CODIGO_UNIDAD_SKU`
      LEFT JOIN `fza_atributos_valores` co
        ON co.`ID_AV` = sac.`ID_AV_SA` AND co.`ID_VA_AV` = 'CO'
      LEFT JOIN `fza_atributos_basicos` atb ON atb.`ID_ATB` = co.`ID_ATB_AV`;

    /* ----------------------------------------------------------------- */
    /* Base de medidas por (artículo, almacén, color). Igual lógica que el */
    /* balance por tallas pero agrupando por color (no por talla). */
    /* ----------------------------------------------------------------- */
    DROP TEMPORARY TABLE IF EXISTS `tmp_bst_base`;
    CREATE TEMPORARY TABLE `tmp_bst_base` (
        `CODIGO_ART`    VARCHAR(20)  NOT NULL,
        `CODIGO_ALM`    VARCHAR(20)  NOT NULL DEFAULT '',
        `COLOR`         VARCHAR(100) NOT NULL DEFAULT '',
        `COLOR_HEX`     VARCHAR(7)   NULL,
        `ORDEN_COLOR`   INT          NOT NULL DEFAULT 0,
        `EXI_INI`       DECIMAL(19,6) NOT NULL DEFAULT 0,
        `ENT`           DECIMAL(19,6) NOT NULL DEFAULT 0,
        `SAL`           DECIMAL(19,6) NOT NULL DEFAULT 0,
        `VEN`           DECIMAL(19,6) NOT NULL DEFAULT 0,
        `EXI_FIN`       DECIMAL(19,6) NOT NULL DEFAULT 0,
        `ENT_COMPRA`    DECIMAL(19,6) NOT NULL DEFAULT 0,
        `ENT_ALBENTRADA` DECIMAL(19,6) NOT NULL DEFAULT 0,
        `ENT_TRASPASO`  DECIMAL(19,6) NOT NULL DEFAULT 0,
        `ENT_DEPOSITO`  DECIMAL(19,6) NOT NULL DEFAULT 0,
        `ENT_REGULAR`   DECIMAL(19,6) NOT NULL DEFAULT 0,
        `SAL_TRASPASO`  DECIMAL(19,6) NOT NULL DEFAULT 0,
        `SAL_DEPOSITO`  DECIMAL(19,6) NOT NULL DEFAULT 0,
        `SAL_ALBVENTA`  DECIMAL(19,6) NOT NULL DEFAULT 0,
        `SAL_VENTA`     DECIMAL(19,6) NOT NULL DEFAULT 0,
        PRIMARY KEY (`CODIGO_ART`, `CODIGO_ALM`, `COLOR`)
    );

    IF p_MODO = 'A' THEN
        /* Acumulados denormalizados del stock actual. */
        INSERT INTO `tmp_bst_base`
            (`CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
             `EXI_INI`, `ENT`, `SAL`, `VEN`, `EXI_FIN`,
             `ENT_COMPRA`, `ENT_ALBENTRADA`, `ENT_TRASPASO`, `ENT_DEPOSITO`,
             `ENT_REGULAR`, `SAL_TRASPASO`, `SAL_DEPOSITO`, `SAL_ALBVENTA`,
             `SAL_VENTA`)
        SELECT s.`CODIGO_ART`, IF(v_por_alm, st.`CODIGO_ALM_STK`, ''),
               s.`COLOR`, MIN(s.`COLOR_HEX`), MIN(s.`ORDEN_COLOR`),
               0,
               SUM(st.`CANTIDAD_ENT_COMPRA_STK` + st.`CANTIDAD_ENT_TRASPASO_STK`
                 + st.`CANTIDAD_ENT_DEPOSITO_STK` + st.`CANTIDAD_ENT_REGULAR_STK`
                 + st.`CANTIDAD_ENT_ALBENTRADA_STK`),
               SUM(st.`CANTIDAD_SAL_TRASPASO_STK` + st.`CANTIDAD_SAL_DEPOSITO_STK`
                 + st.`CANTIDAD_SAL_VENTA_STK` + st.`CANTIDAD_SAL_ALBVENTA_STK`),
               SUM(st.`CANTIDAD_SAL_VENTA_STK` + st.`CANTIDAD_SAL_ALBVENTA_STK`),
               SUM(st.`CANTIDAD_STK`),
               SUM(st.`CANTIDAD_ENT_COMPRA_STK`), SUM(st.`CANTIDAD_ENT_ALBENTRADA_STK`),
               SUM(st.`CANTIDAD_ENT_TRASPASO_STK`), SUM(st.`CANTIDAD_ENT_DEPOSITO_STK`),
               SUM(st.`CANTIDAD_ENT_REGULAR_STK`), SUM(st.`CANTIDAD_SAL_TRASPASO_STK`),
               SUM(st.`CANTIDAD_SAL_DEPOSITO_STK`), SUM(st.`CANTIDAD_SAL_ALBVENTA_STK`),
               SUM(st.`CANTIDAD_SAL_VENTA_STK`)
          FROM `tmp_bst_sku` s
          JOIN `fza_articulos_stockactual` st
            ON st.`CODIGO_UNIDAD_STK` = s.`CODIGO_UNIDAD`
           AND FIND_IN_SET(st.`CODIGO_ALM_STK`, v_alms)
         GROUP BY s.`CODIGO_ART`, IF(v_por_alm, st.`CODIGO_ALM_STK`, ''),
                  s.`COLOR`;
    ELSE
        /* Entre fechas: stock actual + movimientos firmados unificados por */
        /* (unidad, almacén). Si no se agrupa por almacén, ALM = '' y colapsa. */
        INSERT INTO `tmp_bst_base`
            (`CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
             `EXI_INI`, `ENT`, `SAL`, `VEN`, `EXI_FIN`,
             `ENT_COMPRA`, `ENT_ALBENTRADA`, `ENT_TRASPASO`, `ENT_DEPOSITO`,
             `ENT_REGULAR`, `SAL_TRASPASO`, `SAL_DEPOSITO`, `SAL_ALBVENTA`,
             `SAL_VENTA`)
        SELECT s.`CODIGO_ART`, COALESCE(mv.`ALM`, ''),
               s.`COLOR`, MIN(s.`COLOR_HEX`), MIN(s.`ORDEN_COLOR`),
               SUM(COALESCE(mv.`STOCK_NOW`, 0) - COALESCE(mv.`DELTA_DESDE`, 0)),
               SUM(COALESCE(mv.`ENT`, 0)),
               SUM(COALESCE(mv.`SAL`, 0)),
               SUM(COALESCE(mv.`VEN`, 0)),
               SUM(COALESCE(mv.`STOCK_NOW`, 0) - COALESCE(mv.`DELTA_HASTA`, 0)),
               SUM(COALESCE(mv.`ENT_COMPRA`, 0)), SUM(COALESCE(mv.`ENT_ALBENTRADA`, 0)),
               SUM(COALESCE(mv.`ENT_TRASPASO`, 0)), SUM(COALESCE(mv.`ENT_DEPOSITO`, 0)),
               SUM(COALESCE(mv.`ENT_REGULAR`, 0)), SUM(COALESCE(mv.`SAL_TRASPASO`, 0)),
               SUM(COALESCE(mv.`SAL_DEPOSITO`, 0)), SUM(COALESCE(mv.`SAL_ALBVENTA`, 0)),
               SUM(COALESCE(mv.`SAL_VENTA`, 0))
          FROM `tmp_bst_sku` s
          LEFT JOIN (
                SELECT u.`CODIGO_UNIDAD`, u.`ALM`,
                       SUM(u.`STOCK_NOW`)      AS `STOCK_NOW`,
                       SUM(u.`ENT`)            AS `ENT`,
                       SUM(u.`SAL`)            AS `SAL`,
                       SUM(u.`VEN`)            AS `VEN`,
                       SUM(u.`ENT_COMPRA`)     AS `ENT_COMPRA`,
                       SUM(u.`ENT_ALBENTRADA`) AS `ENT_ALBENTRADA`,
                       SUM(u.`ENT_TRASPASO`)   AS `ENT_TRASPASO`,
                       SUM(u.`ENT_DEPOSITO`)   AS `ENT_DEPOSITO`,
                       SUM(u.`ENT_REGULAR`)    AS `ENT_REGULAR`,
                       SUM(u.`SAL_TRASPASO`)   AS `SAL_TRASPASO`,
                       SUM(u.`SAL_DEPOSITO`)   AS `SAL_DEPOSITO`,
                       SUM(u.`SAL_ALBVENTA`)   AS `SAL_ALBVENTA`,
                       SUM(u.`SAL_VENTA`)      AS `SAL_VENTA`,
                       SUM(u.`DELTA_DESDE`)    AS `DELTA_DESDE`,
                       SUM(u.`DELTA_HASTA`)    AS `DELTA_HASTA`
                  FROM (
                        SELECT st2.`CODIGO_UNIDAD_STK` AS `CODIGO_UNIDAD`,
                               IF(v_por_alm, st2.`CODIGO_ALM_STK`, '') AS `ALM`,
                               st2.`CANTIDAD_STK` AS `STOCK_NOW`,
                               0 AS `ENT`, 0 AS `SAL`, 0 AS `VEN`,
                               0 AS `ENT_COMPRA`, 0 AS `ENT_ALBENTRADA`,
                               0 AS `ENT_TRASPASO`, 0 AS `ENT_DEPOSITO`,
                               0 AS `ENT_REGULAR`, 0 AS `SAL_TRASPASO`,
                               0 AS `SAL_DEPOSITO`, 0 AS `SAL_ALBVENTA`,
                               0 AS `SAL_VENTA`, 0 AS `DELTA_DESDE`,
                               0 AS `DELTA_HASTA`
                          FROM `fza_articulos_stockactual` st2
                         WHERE FIND_IN_SET(st2.`CODIGO_ALM_STK`, v_alms)
                        UNION ALL
                        SELECT m.`CODIGO_UNIDAD_MOV`,
                               IF(v_por_alm, m.`CODIGO_ALM_MOV`, ''),
                               0,
                               IF(m.`TIPO_MOV` = 'E'
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(m.`TIPO_MOV` = 'S'
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(m.`TIPO_MOV` = 'S'
                                  AND m.`TIPO_DOC_MOV` IN ('VE', 'FC', 'AV')
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(m.`TIPO_DOC_MOV` = 'AC' AND m.`TIPO_MOV` = 'E'
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(m.`TIPO_DOC_MOV` = 'AE' AND m.`TIPO_MOV` = 'E'
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(m.`TIPO_DOC_MOV` IN ('TR', 'AT') AND m.`TIPO_MOV` = 'E'
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(m.`TIPO_DOC_MOV` = 'DP' AND m.`TIPO_MOV` = 'E'
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(m.`TIPO_DOC_MOV` = 'IN' AND m.`TIPO_MOV` = 'E'
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(m.`TIPO_DOC_MOV` IN ('TR', 'AT') AND m.`TIPO_MOV` = 'S'
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(m.`TIPO_DOC_MOV` = 'DP' AND m.`TIPO_MOV` = 'S'
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(m.`TIPO_DOC_MOV` = 'AV' AND m.`TIPO_MOV` = 'S'
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(m.`TIPO_DOC_MOV` IN ('VE', 'FC') AND m.`TIPO_MOV` = 'S'
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(DATE(m.`FECHA_MOV`) >= v_desde,
                                  IF(m.`TIPO_MOV` = 'E', m.`CANTIDAD_MOV`,
                                     -m.`CANTIDAD_MOV`), 0),
                               IF(DATE(m.`FECHA_MOV`) > v_hasta,
                                  IF(m.`TIPO_MOV` = 'E', m.`CANTIDAD_MOV`,
                                     -m.`CANTIDAD_MOV`), 0)
                          FROM `fza_movimientos_almacen` m
                         WHERE m.`ESACTIVO_MOV` = 'S'
                           AND FIND_IN_SET(m.`CODIGO_ALM_MOV`, v_alms)
                       ) u
                 GROUP BY u.`CODIGO_UNIDAD`, u.`ALM`
               ) mv ON mv.`CODIGO_UNIDAD` = s.`CODIGO_UNIDAD`
         GROUP BY s.`CODIGO_ART`, COALESCE(mv.`ALM`, ''), s.`COLOR`;
    END IF;

    /* Descartar (artículo, color) sin existencias ni movimientos: cubrir todo */
    /* el catálogo si no llenaría el informe de artículos inactivos a cero. */
    DELETE FROM `tmp_bst_base`
     WHERE `EXI_INI` = 0 AND `ENT` = 0 AND `SAL` = 0 AND `VEN` = 0
       AND `EXI_FIN` = 0 AND `ENT_COMPRA` = 0 AND `ENT_ALBENTRADA` = 0
       AND `ENT_TRASPASO` = 0 AND `ENT_DEPOSITO` = 0 AND `ENT_REGULAR` = 0
       AND `SAL_TRASPASO` = 0 AND `SAL_DEPOSITO` = 0 AND `SAL_ALBVENTA` = 0
       AND `SAL_VENTA` = 0;

    /* ----------------------------------------------------------------- */
    /* Desdoblar en bandas (forma larga), igual que el balance por tallas */
    /* pero sin posición de talla. */
    /* ----------------------------------------------------------------- */
    DROP TEMPORARY TABLE IF EXISTS `tmp_bst_medidas`;
    CREATE TEMPORARY TABLE `tmp_bst_medidas` (
        `CODIGO_ART`     VARCHAR(20)  NOT NULL,
        `CODIGO_ALM`     VARCHAR(20)  NOT NULL DEFAULT '',
        `COLOR`          VARCHAR(100) NULL,
        `COLOR_HEX`      VARCHAR(7)   NULL,
        `ORDEN_COLOR`    INT          NOT NULL DEFAULT 0,
        `BANDA`          VARCHAR(20)  NOT NULL,
        `ORDEN_BANDA`    INT          NOT NULL,
        `ETIQUETA_BANDA` VARCHAR(40)  NOT NULL,
        `ES_COSTE`       TINYINT      NOT NULL DEFAULT 0,
        `CANTIDAD`       DECIMAL(19,6) NOT NULL DEFAULT 0,
        KEY `IDX_BST_MED` (`CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `ORDEN_BANDA`)
    );
    /* Existencias iniciales: solo entre fechas. */
    IF p_MODO = 'F' THEN
        INSERT INTO `tmp_bst_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               'EXIINI', 10, 'Existencias iniciales', 1, `EXI_INI`
          FROM `tmp_bst_base`;
    END IF;
    /* Entradas / Salidas agregadas: simplificado (F) o acumulados (A). */
    IF (p_MODO = 'F' AND p_DESGLOSADO = 'N') OR p_MODO = 'A' THEN
        INSERT INTO `tmp_bst_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               'ENT', 20, 'Entradas', 1, `ENT`
          FROM `tmp_bst_base`;
        INSERT INTO `tmp_bst_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               'SAL', 40, 'Salidas', 0, `SAL`
          FROM `tmp_bst_base`;
        INSERT INTO `tmp_bst_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               'VEN', 50, 'Ventas', 0, `VEN`
          FROM `tmp_bst_base`;
    END IF;
    /* Entradas / Salidas desglosadas: solo modo entre fechas desglosado. */
    IF p_MODO = 'F' AND p_DESGLOSADO = 'S' THEN
        INSERT INTO `tmp_bst_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               'ENTCMP', 21, 'Ent. compra', 1, `ENT_COMPRA`
          FROM `tmp_bst_base`;
        INSERT INTO `tmp_bst_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               'ENTALB', 22, 'Alb. entrada', 1, `ENT_ALBENTRADA`
          FROM `tmp_bst_base`;
        INSERT INTO `tmp_bst_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               'ENTTRA', 23, 'Ent. traspaso', 1, `ENT_TRASPASO`
          FROM `tmp_bst_base`;
        INSERT INTO `tmp_bst_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               'ENTDEP', 24, 'Ent. depósito', 1, `ENT_DEPOSITO`
          FROM `tmp_bst_base`;
        INSERT INTO `tmp_bst_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               'ENTREG', 25, 'Regulariz.', 1, `ENT_REGULAR`
          FROM `tmp_bst_base`;
        INSERT INTO `tmp_bst_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               'SALTRA', 41, 'Sal. traspaso', 0, `SAL_TRASPASO`
          FROM `tmp_bst_base`;
        INSERT INTO `tmp_bst_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               'SALDEP', 42, 'Sal. depósito', 0, `SAL_DEPOSITO`
          FROM `tmp_bst_base`;
        INSERT INTO `tmp_bst_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               'SALALB', 43, 'Alb. venta', 0, `SAL_ALBVENTA`
          FROM `tmp_bst_base`;
        INSERT INTO `tmp_bst_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               'VEN', 50, 'Ventas', 0, `SAL_VENTA`
          FROM `tmp_bst_base`;
    END IF;
    /* Existencias finales: siempre. */
    INSERT INTO `tmp_bst_medidas`
    SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
           'EXIFIN', 90, 'Existencias finales', 1, `EXI_FIN`
      FROM `tmp_bst_base`;
    /* Selección de bandas: sin selección = todas las de la configuración. */
    IF p_BANDAS <> '' THEN
        DELETE FROM `tmp_bst_medidas` WHERE NOT FIND_IN_SET(`BANDA`, p_BANDAS);
    END IF;

    /* ----------------------------------------------------------------- */
    /* Salida final: una fila por (artículo, color, banda), enriquecida con */
    /* familia, foto, valoración y columnas de agrupación. */
    /* ----------------------------------------------------------------- */
    SELECT
        COALESCE(fam.`ORDEN_FAM`, 999999)             AS `ORDEN_FAM`,
        art.`CODIGO_FAM_ART`                          AS `CODIGO_FAM`,
        COALESCE(fam.`DESCRIPCION_FAM`,
                 fam.`NOMBRE_FAM_FAM`, art.`CODIGO_FAM_ART`) AS `DESCRIPCION_FAM`,
        p.`CODIGO_ART`                                AS `CODIGO_ART_ART`,
        art.`DESCRIPCION_ART`                         AS `DESCRIPCION_ART`,
        p.`CODIGO_ALM`                                AS `CODIGO_ALM`,
        COALESCE(alm.`NOMBRE_ALM_ALM`, '')            AS `NOMBRE_ALM`,
        prov.`REF_PROVEEDOR_AP`                       AS `REF_PRV`,
        ROUND(COALESCE(NULLIF(cst.`COSTE`, 0), prov.`COSTE_PRV`, 0), 2) AS `COSTE_ART`,
        ROUND(COALESCE(pvp.`PVP`, 0), 2)              AS `PVP_ART`,
        p.`ORDEN_COLOR`, p.`COLOR`, p.`COLOR_HEX`,
        p.`ORDEN_BANDA`, p.`BANDA`, p.`ETIQUETA_BANDA`, p.`ES_COSTE`,
        p.`CANTIDAD`,
        ROUND(IF(p.`BANDA` = 'VEN',
                 IF(p.`CANTIDAD` <> 0,
                    COALESCE(vt.`VEN_IMPORTE`, 0) / p.`CANTIDAD`, 0),
                 IF(p.`ES_COSTE` = 1,
                    COALESCE(NULLIF(cst.`COSTE`, 0), prov.`COSTE_PRV`, 0),
                    COALESCE(pvp.`PVP`, 0))), 2)        AS `PRECIO`,
        /* Importe de la banda. La banda de ventas (VEN) se valora al PRECIO */
        /* REAL de venta (con descuentos, con IVA) tomado de fza_facturas_lineas; */
        /* el resto a coste/PMP o a tarifa según ES_COSTE. */
        ROUND(IF(p.`BANDA` = 'VEN',
                 COALESCE(vt.`VEN_IMPORTE`, 0),
                 p.`CANTIDAD` * IF(p.`ES_COSTE` = 1,
                   COALESCE(NULLIF(cst.`COSTE`, 0), prov.`COSTE_PRV`, 0),
                   COALESCE(pvp.`PVP`, 0))), 2)          AS `IMPORTE`,
        /* Ventas reales (con descuento, con IVA) solo en la banda de ventas */
        /* (VEN); 0 en el resto. Al sumarla por artículo/grupo/total da el */
        /* acumulado de ventas (las existencias se leen banda a banda; las */
        /* ventas hay que irlas sumando). */
        ROUND(IF(p.`BANDA` = 'VEN', COALESCE(vt.`VEN_IMPORTE`, 0), 0), 2)
                                                      AS `VENTAS`,
        CASE p_NIVEL1
            WHEN 'PRV' THEN COALESCE(prov.`CODIGO_PRV`, '')
            WHEN 'FAM' THEN COALESCE(fg.`COD_GRP`, art.`CODIGO_FAM_ART`)
            WHEN 'TMP' THEN COALESCE(tmp.`TEMPORADA`, '')
            WHEN 'ALM' THEN p.`CODIGO_ALM`
            ELSE ''
        END                                           AS `GRUPO1_COD`,
        CASE p_NIVEL1
            WHEN 'PRV' THEN CONCAT('Proveedor: ',
                 COALESCE(NULLIF(prov.`RAZON`, ''), prov.`CODIGO_PRV`,
                          '(sin proveedor)'))
            WHEN 'FAM' THEN CONCAT('Familia: ',
                 COALESCE(fg.`DESC_GRP`, fg.`COD_GRP`,
                          art.`CODIGO_FAM_ART`))
            WHEN 'TMP' THEN CONCAT('Temporada: ',
                 COALESCE(NULLIF(tmp.`TEMPORADA`, ''), '(sin temporada)'))
            WHEN 'ALM' THEN CONCAT('Almacén: ',
                 COALESCE(NULLIF(alm.`NOMBRE_ALM_ALM`, ''), p.`CODIGO_ALM`,
                          '(sin almacén)'))
            ELSE ''
        END                                           AS `GRUPO1_ETIQ`,
        CASE p_NIVEL2
            WHEN 'PRV' THEN COALESCE(prov.`CODIGO_PRV`, '')
            WHEN 'FAM' THEN COALESCE(fg.`COD_GRP`, art.`CODIGO_FAM_ART`)
            WHEN 'TMP' THEN COALESCE(tmp.`TEMPORADA`, '')
            WHEN 'ALM' THEN p.`CODIGO_ALM`
            ELSE ''
        END                                           AS `GRUPO2_COD`,
        CASE p_NIVEL2
            WHEN 'PRV' THEN CONCAT('Proveedor: ',
                 COALESCE(NULLIF(prov.`RAZON`, ''), prov.`CODIGO_PRV`,
                          '(sin proveedor)'))
            WHEN 'FAM' THEN CONCAT('Familia: ',
                 COALESCE(fg.`DESC_GRP`, fg.`COD_GRP`,
                          art.`CODIGO_FAM_ART`))
            WHEN 'TMP' THEN CONCAT('Temporada: ',
                 COALESCE(NULLIF(tmp.`TEMPORADA`, ''), '(sin temporada)'))
            WHEN 'ALM' THEN CONCAT('Almacén: ',
                 COALESCE(NULLIF(alm.`NOMBRE_ALM_ALM`, ''), p.`CODIGO_ALM`,
                          '(sin almacén)'))
            ELSE ''
        END                                           AS `GRUPO2_ETIQ`,
        CASE p_NIVEL3
            WHEN 'PRV' THEN COALESCE(prov.`CODIGO_PRV`, '')
            WHEN 'FAM' THEN COALESCE(fg.`COD_GRP`, art.`CODIGO_FAM_ART`)
            WHEN 'TMP' THEN COALESCE(tmp.`TEMPORADA`, '')
            WHEN 'ALM' THEN p.`CODIGO_ALM`
            ELSE ''
        END                                           AS `GRUPO3_COD`,
        CASE p_NIVEL3
            WHEN 'PRV' THEN CONCAT('Proveedor: ',
                 COALESCE(NULLIF(prov.`RAZON`, ''), prov.`CODIGO_PRV`,
                          '(sin proveedor)'))
            WHEN 'FAM' THEN CONCAT('Familia: ',
                 COALESCE(fg.`DESC_GRP`, fg.`COD_GRP`,
                          art.`CODIGO_FAM_ART`))
            WHEN 'TMP' THEN CONCAT('Temporada: ',
                 COALESCE(NULLIF(tmp.`TEMPORADA`, ''), '(sin temporada)'))
            WHEN 'ALM' THEN CONCAT('Almacén: ',
                 COALESCE(NULLIF(alm.`NOMBRE_ALM_ALM`, ''), p.`CODIGO_ALM`,
                          '(sin almacén)'))
            ELSE ''
        END                                           AS `GRUPO3_ETIQ`
      FROM (
            SELECT m.`CODIGO_ART`, m.`CODIGO_ALM`, m.`COLOR`,
                   MIN(m.`COLOR_HEX`) AS `COLOR_HEX`,
                   MIN(m.`ORDEN_COLOR`) AS `ORDEN_COLOR`,
                   m.`BANDA`, m.`ORDEN_BANDA`, m.`ETIQUETA_BANDA`, m.`ES_COSTE`,
                   SUM(m.`CANTIDAD`) AS `CANTIDAD`
              FROM `tmp_bst_medidas` m
             GROUP BY m.`CODIGO_ART`, m.`CODIGO_ALM`, m.`COLOR`, m.`BANDA`,
                      m.`ORDEN_BANDA`, m.`ETIQUETA_BANDA`, m.`ES_COSTE`
           ) p
      JOIN `fza_articulos` art ON art.`CODIGO_ART_ART` = p.`CODIGO_ART`
      LEFT JOIN `fza_articulos_familias` fam
        ON fam.`CODIGO_FAM_FAM` = art.`CODIGO_FAM_ART`
      LEFT JOIN `tmp_bst_fam_grp` fg ON fg.`CODIGO_FAM` = art.`CODIGO_FAM_ART`
      LEFT JOIN `fza_almacenes` alm ON alm.`CODIGO_ALM_ALM` = p.`CODIGO_ALM`
      LEFT JOIN (
            SELECT t.`CODIGO_ART_ARTTAR` AS `CODIGO_ART`,
                   MAX(t.`PRECIO_FINAL_ARTTAR`) AS `PVP`
              FROM `fza_articulos_tarifas` t
             WHERE t.`CODIGO_TAR_ARTTAR` = v_tarifa
               AND IFNULL(t.`CODIGO_UNIDAD_ARTTAR`, '') = ''
               AND t.`ESACTIVO_ARTTAR` = 'S'
               AND (t.`FECHA_DESDE_ARTTAR` IS NULL
                    OR t.`FECHA_DESDE_ARTTAR` <= CURRENT_DATE)
               AND (t.`FECHA_HASTA_ARTTAR` IS NULL
                    OR t.`FECHA_HASTA_ARTTAR` >= CURRENT_DATE)
             GROUP BY t.`CODIGO_ART_ARTTAR`
           ) pvp ON pvp.`CODIGO_ART` = p.`CODIGO_ART`
      LEFT JOIN (
            SELECT sk.`CODIGO_ART_SKU` AS `CODIGO_ART`,
                   IF(SUM(st.`CANTIDAD_STK`) <> 0,
                      SUM(st.`VALOR_TOTAL_STK`) / SUM(st.`CANTIDAD_STK`), 0) AS `COSTE`
              FROM `fza_articulos_stockactual` st
              JOIN `fza_articulos_skus` sk
                ON sk.`CODIGO_UNIDAD_SKU` = st.`CODIGO_UNIDAD_STK`
             WHERE FIND_IN_SET(st.`CODIGO_ALM_STK`, v_alms)
             GROUP BY sk.`CODIGO_ART_SKU`
           ) cst ON cst.`CODIGO_ART` = p.`CODIGO_ART`
      LEFT JOIN (
            SELECT ap.`CODIGO_ART_AP` AS `CODIGO_ART`,
                   MAX(ap.`REF_PROVEEDOR_AP`)   AS `REF_PROVEEDOR_AP`,
                   MAX(ap.`PRECIO_ULT_COMPRA_AP`) AS `COSTE_PRV`,
                   MAX(ap.`CODIGO_PRV_AP`)      AS `CODIGO_PRV`,
                   MAX(pr.`RAZON_SOCIAL_PRV`)   AS `RAZON`
              FROM `fza_articulos_proveedores` ap
              LEFT JOIN `fza_proveedores` pr
                ON pr.`CODIGO_PRV_PRV` = ap.`CODIGO_PRV_AP`
             WHERE ap.`ESPROVEEDORPRINCIPAL_AP` = 'S'
             GROUP BY ap.`CODIGO_ART_AP`
           ) prov ON prov.`CODIGO_ART` = p.`CODIGO_ART`
      LEFT JOIN (
            SELECT tp.`CODIGO_ART_ART` AS `CODIGO_ART`,
                   MAX(COALESCE(tpv.`PV`, tp.`VALOR_LIBRE_ARTPROP`)) AS `TEMPORADA`
              FROM `fza_articulos_propiedades` tp
              LEFT JOIN `fza_propiedades_valores` tpv
                ON tpv.`ID_PV_ARTPROP` = tp.`ID_PV_ARTPROP`
             WHERE tp.`CODIGO_PROP_ARTPROP` = 'TEMPORADA'
             GROUP BY tp.`CODIGO_ART_ART`
           ) tmp ON tmp.`CODIGO_ART` = p.`CODIGO_ART`
      LEFT JOIN (
            /* Ventas REALES (con descuento, con IVA) por (artículo, almacén, */
            /* color), de las líneas de factura/ticket. Periodo por fecha de */
            /* factura (entre fechas) o histórico (acumulados). */
            SELECT s.`CODIGO_ART`,
                   IF(v_por_alm, fl.`CODIGO_ALM_FACLIN`, '') AS `CODIGO_ALM`,
                   s.`COLOR`,
                   SUM(fl.`CANTIDAD_FACLIN`) AS `VEN_QTY`,
                   SUM(fl.`TOTAL_FACLIN`)    AS `VEN_IMPORTE`
              FROM `fza_facturas_lineas` fl
              JOIN `fza_facturas` f
                ON f.`NUMERO_FAC` = fl.`NUMERO_FAC_FACLIN`
               AND f.`SERIE_FAC` = fl.`SERIE_FAC_FACLIN`
              JOIN `tmp_bst_sku` s
                ON s.`CODIGO_UNIDAD` = fl.`CODIGO_UNIDAD_FACLIN`
             WHERE FIND_IN_SET(fl.`CODIGO_ALM_FACLIN`, v_alms)
               AND (p_MODO = 'A'
                    OR DATE(f.`FECHA_FAC`) BETWEEN v_desde AND v_hasta)
             GROUP BY s.`CODIGO_ART`,
                      IF(v_por_alm, fl.`CODIGO_ALM_FACLIN`, ''), s.`COLOR`
           ) vt ON vt.`CODIGO_ART` = p.`CODIGO_ART`
               AND vt.`CODIGO_ALM` = p.`CODIGO_ALM`
               AND vt.`COLOR` = p.`COLOR`
     ORDER BY `GRUPO1_COD`, `GRUPO2_COD`, `GRUPO3_COD`,
              COALESCE(fam.`ORDEN_FAM`, 999999), art.`CODIGO_FAM_ART`,
              p.`CODIGO_ART`, p.`ORDEN_COLOR`, p.`COLOR`, p.`ORDEN_BANDA`;

    /* Limpieza de temporales. */
    DROP TEMPORARY TABLE IF EXISTS `tmp_bst_medidas`;
    DROP TEMPORARY TABLE IF EXISTS `tmp_bst_base`;
    DROP TEMPORARY TABLE IF EXISTS `tmp_bst_sku`;
    DROP TEMPORARY TABLE IF EXISTS `tmp_bst_arts`;
    DROP TEMPORARY TABLE IF EXISTS `tmp_bst_fam_grp`;
    DROP TEMPORARY TABLE IF EXISTS `tmp_bst_fam`;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_GET_BALANCE_ALMACEN_TALLAS
DROP PROCEDURE IF EXISTS `PRC_GET_BALANCE_ALMACEN_TALLAS`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_GET_BALANCE_ALMACEN_TALLAS`(
    IN `p_MODO`         VARCHAR(1),   /* 'F' entre fechas, 'A' por acumulados */
    IN `p_DESDE`        DATE,         /* inclusive (solo modo 'F') */
    IN `p_HASTA`        DATE,         /* inclusive (solo modo 'F') */
    IN `p_ALMACENES`    TEXT,         /* CSV "01,50" o '' = todos los activos */
    IN `p_FAMILIAS`     TEXT,         /* CSV; '' = todas. Una padre incluye sus hijas */
    IN `p_PROVEEDORES`  TEXT,         /* CSV de códigos de proveedor; '' = todos */
    IN `p_TEMPORADAS`   TEXT,         /* CSV de valores de temporada; '' = todas */
    IN `p_COD_TARIFA`   VARCHAR(20),  /* tarifa para valorar ventas/salidas */
    IN `p_DESGLOSADO`   VARCHAR(1),   /* 'S'/'N' (solo aplica a modo 'F') */
    IN `p_BANDAS`       TEXT,         /* CSV de códigos de banda; '' = todas */
    IN `p_NIVEL1`       VARCHAR(3),   /* 1er nivel de agrupación: PRV/FAM/TMP/ALM/'' */
    IN `p_NIVEL2`       VARCHAR(3),   /* 2o nivel de agrupación */
    IN `p_NIVEL3`       VARCHAR(3),   /* 3er nivel de agrupación */
    IN `p_NIVEL_FAM`    INT           /* nivel del árbol de familias al agrupar */
)
BEGIN
    DECLARE v_alms      TEXT;
    DECLARE v_tarifa    VARCHAR(20);
    DECLARE v_desde     DATE;
    DECLARE v_hasta     DATE;
    DECLARE v_por_alm   BOOLEAN DEFAULT FALSE;  /* TRUE si se agrupa por almacén */
    DECLARE v_nivel_fam INT;                    /* nivel efectivo del árbol fam. */
    /* Normalización de parámetros. */
    SET p_MODO       = IFNULL(NULLIF(p_MODO, ''), 'A');
    SET p_DESGLOSADO  = IFNULL(NULLIF(p_DESGLOSADO, ''), 'N');
    SET p_FAMILIAS    = IFNULL(p_FAMILIAS, '');
    SET p_PROVEEDORES = IFNULL(p_PROVEEDORES, '');
    SET p_TEMPORADAS  = IFNULL(p_TEMPORADAS, '');
    SET p_BANDAS      = IFNULL(p_BANDAS, '');
    /* Niveles de agrupación: normalizados a mayúsculas. Se admiten PRV */
    /* (proveedor), FAM (familia), TMP (temporada) y ALM (almacén); cualquier */
    /* otro valor (o vacío) deshabilita ese nivel. */
    SET p_NIVEL1      = UPPER(IFNULL(p_NIVEL1, ''));
    SET p_NIVEL2      = UPPER(IFNULL(p_NIVEL2, ''));
    SET p_NIVEL3      = UPPER(IFNULL(p_NIVEL3, ''));
    /* Si algún nivel es ALM hay que conservar el almacén en el grano de los */
    /* cálculos (si no, se agregan todos los almacenes filtrados en uno). */
    SET v_por_alm     = (p_NIVEL1 = 'ALM' OR p_NIVEL2 = 'ALM' OR p_NIVEL3 = 'ALM');
    /* Nivel del árbol de familias para agrupar por FAM. <1 (o NULL) = familia */
    /* hoja del artículo (comportamiento clásico); 1 = familia raíz, etc. */
    SET v_nivel_fam   = IF(IFNULL(p_NIVEL_FAM, 0) < 1, 9999, p_NIVEL_FAM);
    SET v_tarifa      = IFNULL(NULLIF(p_COD_TARIFA, ''), 'PVP');
    SET v_desde      = IFNULL(p_DESDE, '1900-01-01');
    SET v_hasta      = IFNULL(p_HASTA, CURRENT_DATE);
    /* Lista efectiva de almacenes (CSV sin comillas, para FIND_IN_SET). */
    /* Sin selección = TODOS los almacenes activos (igual que la lista del */
    /* checklist), no solo los de uso estándar: "nada marcado = todos". */
    IF IFNULL(p_ALMACENES, '') <> '' THEN
        SET v_alms = p_ALMACENES;
    ELSE
        SELECT GROUP_CONCAT(`CODIGO_ALM_ALM`)
          INTO v_alms
          FROM `fza_almacenes`
         WHERE `ESACTIVO_ALM` = 'S';
    END IF;
    SET v_alms = IFNULL(v_alms, '');

    /* ----------------------------------------------------------------- */
    /* Filtros de artículo: familias (con su descendencia), proveedores y */
    /* temporadas. Se materializa en tmp_bat_arts el conjunto de artículos */
    /* que pasan los tres filtros; el resto del SP se restringe a él. */
    /* ----------------------------------------------------------------- */
    /* Familias elegidas expandidas a TODA su descendencia: si se filtra una */
    /* familia padre, entran también sus hijas (CTE recursivo por */
    /* CODIGO_PADRE_FAM). Con p_FAMILIAS vacío sale vacía y no se aplica. */
    DROP TEMPORARY TABLE IF EXISTS `tmp_bat_fam`;
    CREATE TEMPORARY TABLE `tmp_bat_fam` (
        `CODIGO_FAM` VARCHAR(20) NOT NULL PRIMARY KEY
    );
    INSERT IGNORE INTO `tmp_bat_fam` (`CODIGO_FAM`)
    WITH RECURSIVE `fam_tree` AS (
        SELECT `CODIGO_FAM_FAM`
          FROM `fza_articulos_familias`
         WHERE FIND_IN_SET(`CODIGO_FAM_FAM`, p_FAMILIAS)
        UNION ALL
        SELECT f.`CODIGO_FAM_FAM`
          FROM `fza_articulos_familias` f
          JOIN `fam_tree` t ON f.`CODIGO_PADRE_FAM` = t.`CODIGO_FAM_FAM`
    )
    SELECT DISTINCT `CODIGO_FAM_FAM` FROM `fam_tree`;
    /* Mapa de cada familia a su ancestro al nivel pedido (v_nivel_fam), para */
    /* agrupar por FAM "por nivel": si el árbol tiene padres-hijos se puede */
    /* agrupar por la familia raíz (nivel 1), la de 2º nivel, etc. Se construye */
    /* el camino raíz->familia y se toma el código del nivel solicitado (o la */
    /* propia familia si es menos profunda que el nivel pedido). */
    DROP TEMPORARY TABLE IF EXISTS `tmp_bat_fam_grp`;
    CREATE TEMPORARY TABLE `tmp_bat_fam_grp` (
        `CODIGO_FAM` VARCHAR(20)  NOT NULL PRIMARY KEY,
        `COD_GRP`    VARCHAR(20)  NOT NULL,
        `DESC_GRP`   VARCHAR(200) NULL
    );
    INSERT IGNORE INTO `tmp_bat_fam_grp` (`CODIGO_FAM`, `COD_GRP`, `DESC_GRP`)
    WITH RECURSIVE `fam_path` AS (
        SELECT `CODIGO_FAM_FAM` AS `COD`,
               CAST(`CODIGO_FAM_FAM` AS CHAR(1000)) AS `RUTA`
          FROM `fza_articulos_familias`
         WHERE `CODIGO_PADRE_FAM` IS NULL OR `CODIGO_PADRE_FAM` = ''
        UNION ALL
        SELECT f.`CODIGO_FAM_FAM`,
               CONCAT(pa.`RUTA`, '>', f.`CODIGO_FAM_FAM`)
          FROM `fza_articulos_familias` f
          JOIN `fam_path` pa ON f.`CODIGO_PADRE_FAM` = pa.`COD`
    )
    SELECT pa.`COD`,
           SUBSTRING_INDEX(SUBSTRING_INDEX(pa.`RUTA`, '>', v_nivel_fam), '>', -1),
           NULL
      FROM `fam_path` pa;
    /* Descripción del grupo (familia ancestro elegida). */
    UPDATE `tmp_bat_fam_grp` g
      JOIN `fza_articulos_familias` f ON f.`CODIGO_FAM_FAM` = g.`COD_GRP`
       SET g.`DESC_GRP` = COALESCE(f.`DESCRIPCION_FAM`, f.`NOMBRE_FAM_FAM`,
                                   g.`COD_GRP`);
    /* Conjunto de artículos activos que pasan familia, proveedor y temporada. */
    DROP TEMPORARY TABLE IF EXISTS `tmp_bat_arts`;
    CREATE TEMPORARY TABLE `tmp_bat_arts` (
        `CODIGO_ART` VARCHAR(20) NOT NULL PRIMARY KEY
    );
    INSERT IGNORE INTO `tmp_bat_arts` (`CODIGO_ART`)
    SELECT a.`CODIGO_ART_ART`
      FROM `fza_articulos` a
     WHERE a.`ESACTIVO_ART` = 'S'
       AND (p_FAMILIAS = ''
            OR a.`CODIGO_FAM_ART` IN (SELECT `CODIGO_FAM` FROM `tmp_bat_fam`))
       AND (p_PROVEEDORES = ''
            OR EXISTS (SELECT 1 FROM `fza_articulos_proveedores` ap
                        WHERE ap.`CODIGO_ART_AP` = a.`CODIGO_ART_ART`
                          AND FIND_IN_SET(ap.`CODIGO_PRV_AP`, p_PROVEEDORES)))
       AND (p_TEMPORADAS = ''
            OR EXISTS (SELECT 1 FROM `fza_articulos_propiedades` tp
                        LEFT JOIN `fza_propiedades_valores` tpv
                          ON tpv.`ID_PV_ARTPROP` = tp.`ID_PV_ARTPROP`
                        WHERE tp.`CODIGO_ART_ART` = a.`CODIGO_ART_ART`
                          AND tp.`CODIGO_PROP_ARTPROP` = 'TEMPORADA'
                          AND FIND_IN_SET(
                                COALESCE(tpv.`PV`, tp.`VALOR_LIBRE_ARTPROP`),
                                p_TEMPORADAS)));

    /* ----------------------------------------------------------------- */
    /* 1) Posiciones de talla por artículo (T01..T14). */
    /*    Mismo criterio que TfrmStockConsulta.TallasArticulo: el conjunto */
    /*    pivote del artículo (atributo no-color asignado) define el orden */
    /*    de las columnas; si el artículo no tiene asignación, se usan las */
    /*    tallas presentes en sus SKUs como respaldo. */
    /* ----------------------------------------------------------------- */
    DROP TEMPORARY TABLE IF EXISTS `tmp_bat_pos`;
    CREATE TEMPORARY TABLE `tmp_bat_pos` (
        `CODIGO_ART` VARCHAR(20)  NOT NULL,
        `ID_AV`      INT          NOT NULL,
        `ETIQ`       VARCHAR(100) NULL,
        `POSICION`   INT          NOT NULL,
        PRIMARY KEY (`CODIGO_ART`, `ID_AV`)
    );
    /* 1a) Artículos con conjunto pivote asignado. */
    INSERT IGNORE INTO `tmp_bat_pos` (`CODIGO_ART`, `ID_AV`, `ETIQ`, `POSICION`)
    SELECT asg.`CODIGO_ART`, acd.`ID_AV_ACD`, av.`AV`,
           ROW_NUMBER() OVER (PARTITION BY asg.`CODIGO_ART`
                              ORDER BY acd.`ORDEN_ACD`, acd.`ID_AV_ACD`)
      FROM (SELECT a.`CODIGO_ART_ART` AS `CODIGO_ART`,
                   MIN(asa.`ID_AC_ACA`) AS `ID_AC`
              FROM `fza_articulos` a
              JOIN `fza_articulos_conjuntos_asign` asa
                ON asa.`CODIGO_ART_ACA` = a.`CODIGO_ART_ART`
               AND asa.`ID_VA_ACA` <> 'CO'
             WHERE a.`ESACTIVO_ART` = 'S'
               AND a.`CODIGO_ART_ART` IN (SELECT `CODIGO_ART` FROM `tmp_bat_arts`)
             GROUP BY a.`CODIGO_ART_ART`) asg
      JOIN `fza_atributos_conjuntos_det` acd ON acd.`ID_AC_ACD` = asg.`ID_AC`
      JOIN `fza_atributos_valores` av ON av.`ID_AV` = acd.`ID_AV_ACD`;
    /* Artículos ya resueltos (para excluirlos del respaldo sin */
    /* autorreferenciar tmp_bat_pos en el mismo statement). */
    DROP TEMPORARY TABLE IF EXISTS `tmp_bat_pos_arts`;
    CREATE TEMPORARY TABLE `tmp_bat_pos_arts` (
        `CODIGO_ART` VARCHAR(20) NOT NULL PRIMARY KEY
    );
    INSERT IGNORE INTO `tmp_bat_pos_arts`
    SELECT DISTINCT `CODIGO_ART` FROM `tmp_bat_pos`;
    /* 1b) Respaldo: artículos sin asignación -> tallas de sus SKUs. */
    INSERT IGNORE INTO `tmp_bat_pos` (`CODIGO_ART`, `ID_AV`, `ETIQ`, `POSICION`)
    SELECT x.`CODIGO_ART`, x.`ID_AV`, x.`AV`,
           ROW_NUMBER() OVER (PARTITION BY x.`CODIGO_ART`
                              ORDER BY x.`ORDEN_AV`, x.`AV`)
      FROM (SELECT DISTINCT a.`CODIGO_ART_ART` AS `CODIGO_ART`,
                   av.`ID_AV`, av.`AV`, COALESCE(av.`ORDEN_AV`, 0) AS `ORDEN_AV`
              FROM `fza_articulos` a
              JOIN `fza_articulos_skus` sku
                ON sku.`CODIGO_ART_SKU` = a.`CODIGO_ART_ART`
              JOIN `fza_atributos_sku` sa
                ON sa.`CODIGO_UNIDAD_SKU_SA` = sku.`CODIGO_UNIDAD_SKU`
              JOIN `fza_atributos_valores` av
                ON av.`ID_AV` = sa.`ID_AV_SA` AND av.`ID_VA_AV` <> 'CO'
             WHERE a.`ESACTIVO_ART` = 'S'
               AND a.`CODIGO_ART_ART` IN (SELECT `CODIGO_ART` FROM `tmp_bat_arts`)
               AND a.`CODIGO_ART_ART` NOT IN
                   (SELECT `CODIGO_ART` FROM `tmp_bat_pos_arts`)) x;

    /* Etiquetas de cabecera por artículo (ETIQ_T01..ETIQ_T14). */
    DROP TEMPORARY TABLE IF EXISTS `tmp_bat_etiq`;
    CREATE TEMPORARY TABLE `tmp_bat_etiq` AS
    SELECT `CODIGO_ART`,
           MAX(CASE WHEN `POSICION` =  1 THEN `ETIQ` END) AS `ETIQ_T01`,
           MAX(CASE WHEN `POSICION` =  2 THEN `ETIQ` END) AS `ETIQ_T02`,
           MAX(CASE WHEN `POSICION` =  3 THEN `ETIQ` END) AS `ETIQ_T03`,
           MAX(CASE WHEN `POSICION` =  4 THEN `ETIQ` END) AS `ETIQ_T04`,
           MAX(CASE WHEN `POSICION` =  5 THEN `ETIQ` END) AS `ETIQ_T05`,
           MAX(CASE WHEN `POSICION` =  6 THEN `ETIQ` END) AS `ETIQ_T06`,
           MAX(CASE WHEN `POSICION` =  7 THEN `ETIQ` END) AS `ETIQ_T07`,
           MAX(CASE WHEN `POSICION` =  8 THEN `ETIQ` END) AS `ETIQ_T08`,
           MAX(CASE WHEN `POSICION` =  9 THEN `ETIQ` END) AS `ETIQ_T09`,
           MAX(CASE WHEN `POSICION` = 10 THEN `ETIQ` END) AS `ETIQ_T10`,
           MAX(CASE WHEN `POSICION` = 11 THEN `ETIQ` END) AS `ETIQ_T11`,
           MAX(CASE WHEN `POSICION` = 12 THEN `ETIQ` END) AS `ETIQ_T12`,
           MAX(CASE WHEN `POSICION` = 13 THEN `ETIQ` END) AS `ETIQ_T13`,
           MAX(CASE WHEN `POSICION` = 14 THEN `ETIQ` END) AS `ETIQ_T14`
      FROM `tmp_bat_pos`
     GROUP BY `CODIGO_ART`;
    ALTER TABLE `tmp_bat_etiq` ADD PRIMARY KEY (`CODIGO_ART`);

    /* ----------------------------------------------------------------- */
    /* 2) SKUs en juego: artículo + color + posición de talla. Solo las */
    /*    tallas que están en el conjunto pivote (POSICION 1..14). */
    /* ----------------------------------------------------------------- */
    DROP TEMPORARY TABLE IF EXISTS `tmp_bat_sku`;
    CREATE TEMPORARY TABLE `tmp_bat_sku` (
        `CODIGO_UNIDAD` VARCHAR(50)  NOT NULL PRIMARY KEY,
        `CODIGO_ART`    VARCHAR(20)  NOT NULL,
        `COLOR`         VARCHAR(100) NOT NULL DEFAULT '',
        `COLOR_HEX`     VARCHAR(7)   NULL,
        `ORDEN_COLOR`   INT          NOT NULL DEFAULT 0,
        `POSICION`      INT          NOT NULL,
        KEY `IDX_BAT_SKU_ART` (`CODIGO_ART`)
    );
    INSERT IGNORE INTO `tmp_bat_sku`
    SELECT sku.`CODIGO_UNIDAD_SKU`, sku.`CODIGO_ART_SKU`,
           COALESCE(co.`AV`, ''), COALESCE(atb.`HEX_ATB`, ''),
           COALESCE(co.`ORDEN_AV`, 0), p.`POSICION`
      FROM `fza_articulos_skus` sku
      JOIN `fza_articulos` a
        ON a.`CODIGO_ART_ART` = sku.`CODIGO_ART_SKU`
       AND a.`ESACTIVO_ART` = 'S'
      JOIN `fza_atributos_sku` sat
        ON sat.`CODIGO_UNIDAD_SKU_SA` = sku.`CODIGO_UNIDAD_SKU`
      JOIN `fza_atributos_valores` ta
        ON ta.`ID_AV` = sat.`ID_AV_SA` AND ta.`ID_VA_AV` <> 'CO'
      JOIN `tmp_bat_pos` p
        ON p.`CODIGO_ART` = sku.`CODIGO_ART_SKU` AND p.`ID_AV` = ta.`ID_AV`
      LEFT JOIN `fza_atributos_sku` sac
        ON sac.`CODIGO_UNIDAD_SKU_SA` = sku.`CODIGO_UNIDAD_SKU`
      LEFT JOIN `fza_atributos_valores` co
        ON co.`ID_AV` = sac.`ID_AV_SA` AND co.`ID_VA_AV` = 'CO'
      LEFT JOIN `fza_atributos_basicos` atb ON atb.`ID_ATB` = co.`ID_ATB_AV`;

    /* ----------------------------------------------------------------- */
    /* 3) Base de medidas por (artículo, color, posición). Se rellena con */
    /*    ramas distintas según el modo para no calcular ventanas de */
    /*    movimientos cuando se pide acumulados. */
    /* ----------------------------------------------------------------- */
    DROP TEMPORARY TABLE IF EXISTS `tmp_bat_base`;
    CREATE TEMPORARY TABLE `tmp_bat_base` (
        `CODIGO_ART`    VARCHAR(20)  NOT NULL,
        `CODIGO_ALM`    VARCHAR(20)  NOT NULL DEFAULT '',
        `COLOR`         VARCHAR(100) NOT NULL DEFAULT '',
        `COLOR_HEX`     VARCHAR(7)   NULL,
        `ORDEN_COLOR`   INT          NOT NULL DEFAULT 0,
        `POSICION`      INT          NOT NULL,
        `EXI_INI`       DECIMAL(19,6) NOT NULL DEFAULT 0,
        `ENT`           DECIMAL(19,6) NOT NULL DEFAULT 0,
        `SAL`           DECIMAL(19,6) NOT NULL DEFAULT 0,
        `VEN`           DECIMAL(19,6) NOT NULL DEFAULT 0,
        `EXI_FIN`       DECIMAL(19,6) NOT NULL DEFAULT 0,
        `ENT_COMPRA`    DECIMAL(19,6) NOT NULL DEFAULT 0,
        `ENT_ALBENTRADA` DECIMAL(19,6) NOT NULL DEFAULT 0,
        `ENT_TRASPASO`  DECIMAL(19,6) NOT NULL DEFAULT 0,
        `ENT_DEPOSITO`  DECIMAL(19,6) NOT NULL DEFAULT 0,
        `ENT_REGULAR`   DECIMAL(19,6) NOT NULL DEFAULT 0,
        `SAL_TRASPASO`  DECIMAL(19,6) NOT NULL DEFAULT 0,
        `SAL_DEPOSITO`  DECIMAL(19,6) NOT NULL DEFAULT 0,
        `SAL_ALBVENTA`  DECIMAL(19,6) NOT NULL DEFAULT 0,
        `SAL_VENTA`     DECIMAL(19,6) NOT NULL DEFAULT 0,
        PRIMARY KEY (`CODIGO_ART`, `CODIGO_ALM`, `POSICION`, `COLOR`)
    );

    IF p_MODO = 'A' THEN
        /* Acumulados denormalizados del stock actual. */
        INSERT INTO `tmp_bat_base`
            (`CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
             `POSICION`,
             `EXI_INI`, `ENT`, `SAL`, `VEN`, `EXI_FIN`,
             `ENT_COMPRA`, `ENT_ALBENTRADA`, `ENT_TRASPASO`, `ENT_DEPOSITO`,
             `ENT_REGULAR`, `SAL_TRASPASO`, `SAL_DEPOSITO`, `SAL_ALBVENTA`,
             `SAL_VENTA`)
        SELECT s.`CODIGO_ART`, IF(v_por_alm, st.`CODIGO_ALM_STK`, ''),
               s.`COLOR`, MIN(s.`COLOR_HEX`),
               MIN(s.`ORDEN_COLOR`), s.`POSICION`,
               0,
               SUM(st.`CANTIDAD_ENT_COMPRA_STK` + st.`CANTIDAD_ENT_TRASPASO_STK`
                 + st.`CANTIDAD_ENT_DEPOSITO_STK` + st.`CANTIDAD_ENT_REGULAR_STK`
                 + st.`CANTIDAD_ENT_ALBENTRADA_STK`),
               SUM(st.`CANTIDAD_SAL_TRASPASO_STK` + st.`CANTIDAD_SAL_DEPOSITO_STK`
                 + st.`CANTIDAD_SAL_VENTA_STK` + st.`CANTIDAD_SAL_ALBVENTA_STK`),
               SUM(st.`CANTIDAD_SAL_VENTA_STK` + st.`CANTIDAD_SAL_ALBVENTA_STK`),
               SUM(st.`CANTIDAD_STK`),
               SUM(st.`CANTIDAD_ENT_COMPRA_STK`), SUM(st.`CANTIDAD_ENT_ALBENTRADA_STK`),
               SUM(st.`CANTIDAD_ENT_TRASPASO_STK`), SUM(st.`CANTIDAD_ENT_DEPOSITO_STK`),
               SUM(st.`CANTIDAD_ENT_REGULAR_STK`), SUM(st.`CANTIDAD_SAL_TRASPASO_STK`),
               SUM(st.`CANTIDAD_SAL_DEPOSITO_STK`), SUM(st.`CANTIDAD_SAL_ALBVENTA_STK`),
               SUM(st.`CANTIDAD_SAL_VENTA_STK`)
          FROM `tmp_bat_sku` s
          JOIN `fza_articulos_stockactual` st
            ON st.`CODIGO_UNIDAD_STK` = s.`CODIGO_UNIDAD`
           AND FIND_IN_SET(st.`CODIGO_ALM_STK`, v_alms)
         GROUP BY s.`CODIGO_ART`, IF(v_por_alm, st.`CODIGO_ALM_STK`, ''),
                  s.`POSICION`, s.`COLOR`;
    ELSE
        /* Entre fechas: movimientos del periodo + existencias */
        /* reconstruidas desde el stock actual. */
        INSERT INTO `tmp_bat_base`
            (`CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
             `POSICION`,
             `EXI_INI`, `ENT`, `SAL`, `VEN`, `EXI_FIN`,
             `ENT_COMPRA`, `ENT_ALBENTRADA`, `ENT_TRASPASO`, `ENT_DEPOSITO`,
             `ENT_REGULAR`, `SAL_TRASPASO`, `SAL_DEPOSITO`, `SAL_ALBVENTA`,
             `SAL_VENTA`)
        SELECT s.`CODIGO_ART`, COALESCE(mv.`ALM`, ''),
               s.`COLOR`, MIN(s.`COLOR_HEX`),
               MIN(s.`ORDEN_COLOR`), s.`POSICION`,
               /* Existencias iniciales: stock actual menos movimientos */
               /* firmados desde p_DESDE (inclusive). */
               SUM(COALESCE(mv.`STOCK_NOW`, 0) - COALESCE(mv.`DELTA_DESDE`, 0)),
               SUM(COALESCE(mv.`ENT`, 0)),
               SUM(COALESCE(mv.`SAL`, 0)),
               SUM(COALESCE(mv.`VEN`, 0)),
               /* Existencias finales: stock actual menos movimientos */
               /* firmados posteriores a p_HASTA. */
               SUM(COALESCE(mv.`STOCK_NOW`, 0) - COALESCE(mv.`DELTA_HASTA`, 0)),
               SUM(COALESCE(mv.`ENT_COMPRA`, 0)), SUM(COALESCE(mv.`ENT_ALBENTRADA`, 0)),
               SUM(COALESCE(mv.`ENT_TRASPASO`, 0)), SUM(COALESCE(mv.`ENT_DEPOSITO`, 0)),
               SUM(COALESCE(mv.`ENT_REGULAR`, 0)), SUM(COALESCE(mv.`SAL_TRASPASO`, 0)),
               SUM(COALESCE(mv.`SAL_DEPOSITO`, 0)), SUM(COALESCE(mv.`SAL_ALBVENTA`, 0)),
               SUM(COALESCE(mv.`SAL_VENTA`, 0))
          FROM `tmp_bat_sku` s
          LEFT JOIN (
                /* Stock actual y movimientos firmados unificados por unidad y */
                /* almacén (UNION ALL para sumarlos en el mismo grano). Si no se */
                /* agrupa por almacén, ALM = '' y todo colapsa en un único */
                /* bucket (resultado idéntico al cálculo agregado anterior). */
                SELECT u.`CODIGO_UNIDAD`, u.`ALM`,
                       SUM(u.`STOCK_NOW`)      AS `STOCK_NOW`,
                       SUM(u.`ENT`)            AS `ENT`,
                       SUM(u.`SAL`)            AS `SAL`,
                       SUM(u.`VEN`)            AS `VEN`,
                       SUM(u.`ENT_COMPRA`)     AS `ENT_COMPRA`,
                       SUM(u.`ENT_ALBENTRADA`) AS `ENT_ALBENTRADA`,
                       SUM(u.`ENT_TRASPASO`)   AS `ENT_TRASPASO`,
                       SUM(u.`ENT_DEPOSITO`)   AS `ENT_DEPOSITO`,
                       SUM(u.`ENT_REGULAR`)    AS `ENT_REGULAR`,
                       SUM(u.`SAL_TRASPASO`)   AS `SAL_TRASPASO`,
                       SUM(u.`SAL_DEPOSITO`)   AS `SAL_DEPOSITO`,
                       SUM(u.`SAL_ALBVENTA`)   AS `SAL_ALBVENTA`,
                       SUM(u.`SAL_VENTA`)      AS `SAL_VENTA`,
                       SUM(u.`DELTA_DESDE`)    AS `DELTA_DESDE`,
                       SUM(u.`DELTA_HASTA`)    AS `DELTA_HASTA`
                  FROM (
                        SELECT st2.`CODIGO_UNIDAD_STK` AS `CODIGO_UNIDAD`,
                               IF(v_por_alm, st2.`CODIGO_ALM_STK`, '') AS `ALM`,
                               st2.`CANTIDAD_STK` AS `STOCK_NOW`,
                               0 AS `ENT`, 0 AS `SAL`, 0 AS `VEN`,
                               0 AS `ENT_COMPRA`, 0 AS `ENT_ALBENTRADA`,
                               0 AS `ENT_TRASPASO`, 0 AS `ENT_DEPOSITO`,
                               0 AS `ENT_REGULAR`, 0 AS `SAL_TRASPASO`,
                               0 AS `SAL_DEPOSITO`, 0 AS `SAL_ALBVENTA`,
                               0 AS `SAL_VENTA`, 0 AS `DELTA_DESDE`,
                               0 AS `DELTA_HASTA`
                          FROM `fza_articulos_stockactual` st2
                         WHERE FIND_IN_SET(st2.`CODIGO_ALM_STK`, v_alms)
                        UNION ALL
                        SELECT m.`CODIGO_UNIDAD_MOV`,
                               IF(v_por_alm, m.`CODIGO_ALM_MOV`, ''),
                               0,
                               IF(m.`TIPO_MOV` = 'E'
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(m.`TIPO_MOV` = 'S'
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(m.`TIPO_MOV` = 'S'
                                  AND m.`TIPO_DOC_MOV` IN ('VE', 'FC', 'AV')
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(m.`TIPO_DOC_MOV` = 'AC' AND m.`TIPO_MOV` = 'E'
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(m.`TIPO_DOC_MOV` = 'AE' AND m.`TIPO_MOV` = 'E'
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(m.`TIPO_DOC_MOV` IN ('TR', 'AT') AND m.`TIPO_MOV` = 'E'
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(m.`TIPO_DOC_MOV` = 'DP' AND m.`TIPO_MOV` = 'E'
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(m.`TIPO_DOC_MOV` = 'IN' AND m.`TIPO_MOV` = 'E'
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(m.`TIPO_DOC_MOV` IN ('TR', 'AT') AND m.`TIPO_MOV` = 'S'
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(m.`TIPO_DOC_MOV` = 'DP' AND m.`TIPO_MOV` = 'S'
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(m.`TIPO_DOC_MOV` = 'AV' AND m.`TIPO_MOV` = 'S'
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(m.`TIPO_DOC_MOV` IN ('VE', 'FC') AND m.`TIPO_MOV` = 'S'
                                  AND DATE(m.`FECHA_MOV`) BETWEEN v_desde AND v_hasta,
                                  m.`CANTIDAD_MOV`, 0),
                               IF(DATE(m.`FECHA_MOV`) >= v_desde,
                                  IF(m.`TIPO_MOV` = 'E', m.`CANTIDAD_MOV`,
                                     -m.`CANTIDAD_MOV`), 0),
                               IF(DATE(m.`FECHA_MOV`) > v_hasta,
                                  IF(m.`TIPO_MOV` = 'E', m.`CANTIDAD_MOV`,
                                     -m.`CANTIDAD_MOV`), 0)
                          FROM `fza_movimientos_almacen` m
                         WHERE m.`ESACTIVO_MOV` = 'S'
                           AND FIND_IN_SET(m.`CODIGO_ALM_MOV`, v_alms)
                       ) u
                 GROUP BY u.`CODIGO_UNIDAD`, u.`ALM`
               ) mv ON mv.`CODIGO_UNIDAD` = s.`CODIGO_UNIDAD`
         GROUP BY s.`CODIGO_ART`, COALESCE(mv.`ALM`, ''),
                  s.`POSICION`, s.`COLOR`;
    END IF;

    /* ----------------------------------------------------------------- */
    /* 4) Desdoblar en bandas (forma larga). Cada banda es un INSERT */
    /*    independiente (referencia tmp_bat_base una sola vez) y se filtra */
    /*    por modo/desglosado. ES_COSTE marca cómo se valora la banda. */
    /*    ORDEN_BANDA fija el orden vertical del informe. */
    /* ----------------------------------------------------------------- */
    DROP TEMPORARY TABLE IF EXISTS `tmp_bat_medidas`;
    CREATE TEMPORARY TABLE `tmp_bat_medidas` (
        `CODIGO_ART`     VARCHAR(20)  NOT NULL,
        `CODIGO_ALM`     VARCHAR(20)  NOT NULL DEFAULT '',
        `COLOR`          VARCHAR(100) NULL,
        `COLOR_HEX`      VARCHAR(7)   NULL,
        `ORDEN_COLOR`    INT          NOT NULL DEFAULT 0,
        `POSICION`       INT          NOT NULL,
        `BANDA`          VARCHAR(20)  NOT NULL,
        `ORDEN_BANDA`    INT          NOT NULL,
        `ETIQUETA_BANDA` VARCHAR(40)  NOT NULL,
        `ES_COSTE`       TINYINT      NOT NULL DEFAULT 0,
        `CANTIDAD`       DECIMAL(19,6) NOT NULL DEFAULT 0,
        KEY `IDX_BAT_MED` (`CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `ORDEN_BANDA`)
    );

    /* Existencias iniciales: solo entre fechas. */
    IF p_MODO = 'F' THEN
        INSERT INTO `tmp_bat_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               `POSICION`,
               'EXIINI', 10, 'Existencias iniciales', 1, `EXI_INI`
          FROM `tmp_bat_base`;
    END IF;
    /* Entradas / Salidas agregadas: simplificado (F) o acumulados (A). */
    IF (p_MODO = 'F' AND p_DESGLOSADO = 'N') OR p_MODO = 'A' THEN
        INSERT INTO `tmp_bat_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               `POSICION`,
               'ENT', 20, 'Entradas', 1, `ENT`
          FROM `tmp_bat_base`;
        INSERT INTO `tmp_bat_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               `POSICION`,
               'SAL', 40, 'Salidas', 0, `SAL`
          FROM `tmp_bat_base`;
        INSERT INTO `tmp_bat_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               `POSICION`,
               'VEN', 50, 'Ventas', 0, `VEN`
          FROM `tmp_bat_base`;
    END IF;
    /* Entradas / Salidas desglosadas: solo modo entre fechas desglosado. */
    /* Mismos subtipos que la consulta de stock (Ctrl+U). */
    IF p_MODO = 'F' AND p_DESGLOSADO = 'S' THEN
        INSERT INTO `tmp_bat_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               `POSICION`,
               'ENTCMP', 21, 'Ent. compra', 1, `ENT_COMPRA`
          FROM `tmp_bat_base`;
        INSERT INTO `tmp_bat_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               `POSICION`,
               'ENTALB', 22, 'Alb. entrada', 1, `ENT_ALBENTRADA`
          FROM `tmp_bat_base`;
        INSERT INTO `tmp_bat_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               `POSICION`,
               'ENTTRA', 23, 'Ent. traspaso', 1, `ENT_TRASPASO`
          FROM `tmp_bat_base`;
        INSERT INTO `tmp_bat_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               `POSICION`,
               'ENTDEP', 24, 'Ent. depósito', 1, `ENT_DEPOSITO`
          FROM `tmp_bat_base`;
        INSERT INTO `tmp_bat_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               `POSICION`,
               'ENTREG', 25, 'Regulariz.', 1, `ENT_REGULAR`
          FROM `tmp_bat_base`;
        INSERT INTO `tmp_bat_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               `POSICION`,
               'SALTRA', 41, 'Sal. traspaso', 0, `SAL_TRASPASO`
          FROM `tmp_bat_base`;
        INSERT INTO `tmp_bat_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               `POSICION`,
               'SALDEP', 42, 'Sal. depósito', 0, `SAL_DEPOSITO`
          FROM `tmp_bat_base`;
        INSERT INTO `tmp_bat_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               `POSICION`,
               'SALALB', 43, 'Alb. venta', 0, `SAL_ALBVENTA`
          FROM `tmp_bat_base`;
        INSERT INTO `tmp_bat_medidas`
        SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
               `POSICION`,
               'VEN', 50, 'Ventas', 0, `SAL_VENTA`
          FROM `tmp_bat_base`;
    END IF;
    /* Existencias finales: siempre. */
    INSERT INTO `tmp_bat_medidas`
    SELECT `CODIGO_ART`, `CODIGO_ALM`, `COLOR`, `COLOR_HEX`, `ORDEN_COLOR`,
           `POSICION`,
           'EXIFIN', 90, 'Existencias finales', 1, `EXI_FIN`
      FROM `tmp_bat_base`;
    /* Selección de bandas: sin selección = todas las de la configuración */
    /* (modo/detalle). FIND_IN_SET sobre el código de banda. */
    IF p_BANDAS <> '' THEN
        DELETE FROM `tmp_bat_medidas` WHERE NOT FIND_IN_SET(`BANDA`, p_BANDAS);
    END IF;

    /* ----------------------------------------------------------------- */
    /* 5) Pivote final por (artículo, color, banda) y enriquecido con */
    /*    familia, etiquetas de cabecera, foto y valoración. El pivote */
    /*    va en una subconsulta para no mezclar agregados con columnas */
    /*    de adorno (ONLY_FULL_GROUP_BY-safe). */
    /* ----------------------------------------------------------------- */
    SELECT
        COALESCE(fam.`ORDEN_FAM`, 999999)             AS `ORDEN_FAM`,
        art.`CODIGO_FAM_ART`                          AS `CODIGO_FAM`,
        COALESCE(fam.`DESCRIPCION_FAM`,
                 fam.`NOMBRE_FAM_FAM`, art.`CODIGO_FAM_ART`) AS `DESCRIPCION_FAM`,
        p.`CODIGO_ART`                                AS `CODIGO_ART_ART`,
        art.`DESCRIPCION_ART`                         AS `DESCRIPCION_ART`,
        p.`CODIGO_ALM`                                AS `CODIGO_ALM`,
        COALESCE(alm.`NOMBRE_ALM_ALM`, '')            AS `NOMBRE_ALM`,
        prov.`REF_PROVEEDOR_AP`                       AS `REF_PRV`,
        ROUND(COALESCE(NULLIF(cst.`COSTE`, 0), prov.`COSTE_PRV`, 0), 2) AS `COSTE_ART`,
        ROUND(COALESCE(pvp.`PVP`, 0), 2)              AS `PVP_ART`,
        p.`ORDEN_COLOR`, p.`COLOR`, p.`COLOR_HEX`,
        p.`ORDEN_BANDA`, p.`BANDA`, p.`ETIQUETA_BANDA`, p.`ES_COSTE`,
        et.`ETIQ_T01`, et.`ETIQ_T02`, et.`ETIQ_T03`, et.`ETIQ_T04`,
        et.`ETIQ_T05`, et.`ETIQ_T06`, et.`ETIQ_T07`, et.`ETIQ_T08`,
        et.`ETIQ_T09`, et.`ETIQ_T10`, et.`ETIQ_T11`, et.`ETIQ_T12`,
        et.`ETIQ_T13`, et.`ETIQ_T14`,
        p.`T01`, p.`T02`, p.`T03`, p.`T04`, p.`T05`, p.`T06`, p.`T07`,
        p.`T08`, p.`T09`, p.`T10`, p.`T11`, p.`T12`, p.`T13`, p.`T14`,
        p.`CANTIDAD`,
        ROUND(IF(p.`BANDA` = 'VEN',
                 IF(p.`CANTIDAD` <> 0,
                    COALESCE(vt.`VEN_IMPORTE`, 0) / p.`CANTIDAD`, 0),
                 IF(p.`ES_COSTE` = 1,
                    COALESCE(NULLIF(cst.`COSTE`, 0), prov.`COSTE_PRV`, 0),
                    COALESCE(pvp.`PVP`, 0))), 2)        AS `PRECIO`,
        /* Importe de la banda. La banda de ventas (VEN) se valora al PRECIO */
        /* REAL de venta (con descuentos, con IVA) tomado de fza_facturas_lineas; */
        /* el resto a coste/PMP o a tarifa según ES_COSTE. */
        ROUND(IF(p.`BANDA` = 'VEN',
                 COALESCE(vt.`VEN_IMPORTE`, 0),
                 p.`CANTIDAD` * IF(p.`ES_COSTE` = 1,
                   COALESCE(NULLIF(cst.`COSTE`, 0), prov.`COSTE_PRV`, 0),
                   COALESCE(pvp.`PVP`, 0))), 2)          AS `IMPORTE`,
        /* Ventas reales (con descuento, con IVA) solo en la banda de ventas */
        /* (VEN); 0 en el resto. Al sumarla por artículo/grupo/total da el */
        /* acumulado de ventas (las existencias se leen banda a banda; las */
        /* ventas hay que irlas sumando). */
        ROUND(IF(p.`BANDA` = 'VEN', COALESCE(vt.`VEN_IMPORTE`, 0), 0), 2)
                                                      AS `VENTAS`,
        /* Niveles de agrupación configurables. GRUPOn_COD identifica el grupo */
        /* (para el corte y el orden); GRUPOn_ETIQ es la etiqueta a mostrar en */
        /* la cabecera/resumen. Si el nivel no está activo (''), salen vacíos y */
        /* el cliente no dibuja banda de grupo a ese nivel. */
        CASE p_NIVEL1
            WHEN 'PRV' THEN COALESCE(prov.`CODIGO_PRV`, '')
            WHEN 'FAM' THEN COALESCE(fg.`COD_GRP`, art.`CODIGO_FAM_ART`)
            WHEN 'TMP' THEN COALESCE(tmp.`TEMPORADA`, '')
            WHEN 'ALM' THEN p.`CODIGO_ALM`
            ELSE ''
        END                                           AS `GRUPO1_COD`,
        CASE p_NIVEL1
            WHEN 'PRV' THEN CONCAT('Proveedor: ',
                 COALESCE(NULLIF(prov.`RAZON`, ''), prov.`CODIGO_PRV`,
                          '(sin proveedor)'))
            WHEN 'FAM' THEN CONCAT('Familia: ',
                 COALESCE(fg.`DESC_GRP`, fg.`COD_GRP`,
                          art.`CODIGO_FAM_ART`))
            WHEN 'TMP' THEN CONCAT('Temporada: ',
                 COALESCE(NULLIF(tmp.`TEMPORADA`, ''), '(sin temporada)'))
            WHEN 'ALM' THEN CONCAT('Almacén: ',
                 COALESCE(NULLIF(alm.`NOMBRE_ALM_ALM`, ''), p.`CODIGO_ALM`,
                          '(sin almacén)'))
            ELSE ''
        END                                           AS `GRUPO1_ETIQ`,
        CASE p_NIVEL2
            WHEN 'PRV' THEN COALESCE(prov.`CODIGO_PRV`, '')
            WHEN 'FAM' THEN COALESCE(fg.`COD_GRP`, art.`CODIGO_FAM_ART`)
            WHEN 'TMP' THEN COALESCE(tmp.`TEMPORADA`, '')
            WHEN 'ALM' THEN p.`CODIGO_ALM`
            ELSE ''
        END                                           AS `GRUPO2_COD`,
        CASE p_NIVEL2
            WHEN 'PRV' THEN CONCAT('Proveedor: ',
                 COALESCE(NULLIF(prov.`RAZON`, ''), prov.`CODIGO_PRV`,
                          '(sin proveedor)'))
            WHEN 'FAM' THEN CONCAT('Familia: ',
                 COALESCE(fg.`DESC_GRP`, fg.`COD_GRP`,
                          art.`CODIGO_FAM_ART`))
            WHEN 'TMP' THEN CONCAT('Temporada: ',
                 COALESCE(NULLIF(tmp.`TEMPORADA`, ''), '(sin temporada)'))
            WHEN 'ALM' THEN CONCAT('Almacén: ',
                 COALESCE(NULLIF(alm.`NOMBRE_ALM_ALM`, ''), p.`CODIGO_ALM`,
                          '(sin almacén)'))
            ELSE ''
        END                                           AS `GRUPO2_ETIQ`,
        CASE p_NIVEL3
            WHEN 'PRV' THEN COALESCE(prov.`CODIGO_PRV`, '')
            WHEN 'FAM' THEN COALESCE(fg.`COD_GRP`, art.`CODIGO_FAM_ART`)
            WHEN 'TMP' THEN COALESCE(tmp.`TEMPORADA`, '')
            WHEN 'ALM' THEN p.`CODIGO_ALM`
            ELSE ''
        END                                           AS `GRUPO3_COD`,
        CASE p_NIVEL3
            WHEN 'PRV' THEN CONCAT('Proveedor: ',
                 COALESCE(NULLIF(prov.`RAZON`, ''), prov.`CODIGO_PRV`,
                          '(sin proveedor)'))
            WHEN 'FAM' THEN CONCAT('Familia: ',
                 COALESCE(fg.`DESC_GRP`, fg.`COD_GRP`,
                          art.`CODIGO_FAM_ART`))
            WHEN 'TMP' THEN CONCAT('Temporada: ',
                 COALESCE(NULLIF(tmp.`TEMPORADA`, ''), '(sin temporada)'))
            WHEN 'ALM' THEN CONCAT('Almacén: ',
                 COALESCE(NULLIF(alm.`NOMBRE_ALM_ALM`, ''), p.`CODIGO_ALM`,
                          '(sin almacén)'))
            ELSE ''
        END                                           AS `GRUPO3_ETIQ`
      FROM (
            SELECT m.`CODIGO_ART`, m.`CODIGO_ALM`, m.`COLOR`,
                   MIN(m.`COLOR_HEX`) AS `COLOR_HEX`,
                   MIN(m.`ORDEN_COLOR`) AS `ORDEN_COLOR`,
                   m.`BANDA`, m.`ORDEN_BANDA`, m.`ETIQUETA_BANDA`, m.`ES_COSTE`,
                   SUM(IF(m.`POSICION` =  1, m.`CANTIDAD`, 0)) AS `T01`,
                   SUM(IF(m.`POSICION` =  2, m.`CANTIDAD`, 0)) AS `T02`,
                   SUM(IF(m.`POSICION` =  3, m.`CANTIDAD`, 0)) AS `T03`,
                   SUM(IF(m.`POSICION` =  4, m.`CANTIDAD`, 0)) AS `T04`,
                   SUM(IF(m.`POSICION` =  5, m.`CANTIDAD`, 0)) AS `T05`,
                   SUM(IF(m.`POSICION` =  6, m.`CANTIDAD`, 0)) AS `T06`,
                   SUM(IF(m.`POSICION` =  7, m.`CANTIDAD`, 0)) AS `T07`,
                   SUM(IF(m.`POSICION` =  8, m.`CANTIDAD`, 0)) AS `T08`,
                   SUM(IF(m.`POSICION` =  9, m.`CANTIDAD`, 0)) AS `T09`,
                   SUM(IF(m.`POSICION` = 10, m.`CANTIDAD`, 0)) AS `T10`,
                   SUM(IF(m.`POSICION` = 11, m.`CANTIDAD`, 0)) AS `T11`,
                   SUM(IF(m.`POSICION` = 12, m.`CANTIDAD`, 0)) AS `T12`,
                   SUM(IF(m.`POSICION` = 13, m.`CANTIDAD`, 0)) AS `T13`,
                   SUM(IF(m.`POSICION` = 14, m.`CANTIDAD`, 0)) AS `T14`,
                   SUM(m.`CANTIDAD`) AS `CANTIDAD`
              FROM `tmp_bat_medidas` m
             GROUP BY m.`CODIGO_ART`, m.`CODIGO_ALM`, m.`COLOR`, m.`BANDA`,
                      m.`ORDEN_BANDA`, m.`ETIQUETA_BANDA`, m.`ES_COSTE`
           ) p
      JOIN `fza_articulos` art ON art.`CODIGO_ART_ART` = p.`CODIGO_ART`
      LEFT JOIN `fza_articulos_familias` fam
        ON fam.`CODIGO_FAM_FAM` = art.`CODIGO_FAM_ART`
      LEFT JOIN `tmp_bat_fam_grp` fg ON fg.`CODIGO_FAM` = art.`CODIGO_FAM_ART`
      LEFT JOIN `fza_almacenes` alm ON alm.`CODIGO_ALM_ALM` = p.`CODIGO_ALM`
      LEFT JOIN `tmp_bat_etiq` et ON et.`CODIGO_ART` = p.`CODIGO_ART`
      LEFT JOIN (
            SELECT t.`CODIGO_ART_ARTTAR` AS `CODIGO_ART`,
                   MAX(t.`PRECIO_FINAL_ARTTAR`) AS `PVP`
              FROM `fza_articulos_tarifas` t
             WHERE t.`CODIGO_TAR_ARTTAR` = v_tarifa
               AND IFNULL(t.`CODIGO_UNIDAD_ARTTAR`, '') = ''
               AND t.`ESACTIVO_ARTTAR` = 'S'
               AND (t.`FECHA_DESDE_ARTTAR` IS NULL
                    OR t.`FECHA_DESDE_ARTTAR` <= CURRENT_DATE)
               AND (t.`FECHA_HASTA_ARTTAR` IS NULL
                    OR t.`FECHA_HASTA_ARTTAR` >= CURRENT_DATE)
             GROUP BY t.`CODIGO_ART_ARTTAR`
           ) pvp ON pvp.`CODIGO_ART` = p.`CODIGO_ART`
      LEFT JOIN (
            SELECT sk.`CODIGO_ART_SKU` AS `CODIGO_ART`,
                   SUM(st.`VALOR_TOTAL_STK`) AS `VAL`,
                   SUM(st.`CANTIDAD_STK`)    AS `CAN`,
                   IF(SUM(st.`CANTIDAD_STK`) <> 0,
                      SUM(st.`VALOR_TOTAL_STK`) / SUM(st.`CANTIDAD_STK`), 0) AS `COSTE`
              FROM `fza_articulos_stockactual` st
              JOIN `fza_articulos_skus` sk
                ON sk.`CODIGO_UNIDAD_SKU` = st.`CODIGO_UNIDAD_STK`
             WHERE FIND_IN_SET(st.`CODIGO_ALM_STK`, v_alms)
             GROUP BY sk.`CODIGO_ART_SKU`
           ) cst ON cst.`CODIGO_ART` = p.`CODIGO_ART`
      LEFT JOIN (
            SELECT ap.`CODIGO_ART_AP` AS `CODIGO_ART`,
                   MAX(ap.`REF_PROVEEDOR_AP`)   AS `REF_PROVEEDOR_AP`,
                   MAX(ap.`PRECIO_ULT_COMPRA_AP`) AS `COSTE_PRV`,
                   MAX(ap.`CODIGO_PRV_AP`)      AS `CODIGO_PRV`,
                   MAX(pr.`RAZON_SOCIAL_PRV`)   AS `RAZON`
              FROM `fza_articulos_proveedores` ap
              LEFT JOIN `fza_proveedores` pr
                ON pr.`CODIGO_PRV_PRV` = ap.`CODIGO_PRV_AP`
             WHERE ap.`ESPROVEEDORPRINCIPAL_AP` = 'S'
             GROUP BY ap.`CODIGO_ART_AP`
           ) prov ON prov.`CODIGO_ART` = p.`CODIGO_ART`
      LEFT JOIN (
            SELECT tp.`CODIGO_ART_ART` AS `CODIGO_ART`,
                   MAX(COALESCE(tpv.`PV`, tp.`VALOR_LIBRE_ARTPROP`)) AS `TEMPORADA`
              FROM `fza_articulos_propiedades` tp
              LEFT JOIN `fza_propiedades_valores` tpv
                ON tpv.`ID_PV_ARTPROP` = tp.`ID_PV_ARTPROP`
             WHERE tp.`CODIGO_PROP_ARTPROP` = 'TEMPORADA'
             GROUP BY tp.`CODIGO_ART_ART`
           ) tmp ON tmp.`CODIGO_ART` = p.`CODIGO_ART`
      LEFT JOIN (
            /* Ventas REALES (con descuento, con IVA) por (artículo, almacén, */
            /* color), de las líneas de factura/ticket. Periodo por fecha de */
            /* factura (entre fechas) o histórico (acumulados). Se enlaza el SKU */
            /* de la línea a tmp_bat_sku para resolver artículo/color y restringir */
            /* a los artículos filtrados. */
            SELECT s.`CODIGO_ART`,
                   IF(v_por_alm, fl.`CODIGO_ALM_FACLIN`, '') AS `CODIGO_ALM`,
                   s.`COLOR`,
                   SUM(fl.`CANTIDAD_FACLIN`) AS `VEN_QTY`,
                   SUM(fl.`TOTAL_FACLIN`)    AS `VEN_IMPORTE`
              FROM `fza_facturas_lineas` fl
              JOIN `fza_facturas` f
                ON f.`NUMERO_FAC` = fl.`NUMERO_FAC_FACLIN`
               AND f.`SERIE_FAC` = fl.`SERIE_FAC_FACLIN`
              JOIN `tmp_bat_sku` s
                ON s.`CODIGO_UNIDAD` = fl.`CODIGO_UNIDAD_FACLIN`
             WHERE FIND_IN_SET(fl.`CODIGO_ALM_FACLIN`, v_alms)
               AND (p_MODO = 'A'
                    OR DATE(f.`FECHA_FAC`) BETWEEN v_desde AND v_hasta)
             GROUP BY s.`CODIGO_ART`,
                      IF(v_por_alm, fl.`CODIGO_ALM_FACLIN`, ''), s.`COLOR`
           ) vt ON vt.`CODIGO_ART` = p.`CODIGO_ART`
               AND vt.`CODIGO_ALM` = p.`CODIGO_ALM`
               AND vt.`COLOR` = p.`COLOR`
     ORDER BY `GRUPO1_COD`, `GRUPO2_COD`, `GRUPO3_COD`,
              COALESCE(fam.`ORDEN_FAM`, 999999), art.`CODIGO_FAM_ART`,
              p.`CODIGO_ART`, p.`ORDEN_COLOR`, p.`COLOR`, p.`ORDEN_BANDA`;

    /* Limpieza de temporales para no arrastrarlas en la sesión. */
    DROP TEMPORARY TABLE IF EXISTS `tmp_bat_medidas`;
    DROP TEMPORARY TABLE IF EXISTS `tmp_bat_base`;
    DROP TEMPORARY TABLE IF EXISTS `tmp_bat_sku`;
    DROP TEMPORARY TABLE IF EXISTS `tmp_bat_etiq`;
    DROP TEMPORARY TABLE IF EXISTS `tmp_bat_pos_arts`;
    DROP TEMPORARY TABLE IF EXISTS `tmp_bat_pos`;
    DROP TEMPORARY TABLE IF EXISTS `tmp_bat_arts`;
    DROP TEMPORARY TABLE IF EXISTS `tmp_bat_fam_grp`;
    DROP TEMPORARY TABLE IF EXISTS `tmp_bat_fam`;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_GET_CAJA_STOCK_PIVOTADO
DROP PROCEDURE IF EXISTS `PRC_GET_CAJA_STOCK_PIVOTADO`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_GET_CAJA_STOCK_PIVOTADO`(IN p_input VARCHAR(50))
BEGIN
    DECLARE v_codigo_articulo VARCHAR(20);
    DECLARE v_es_sku BOOLEAN DEFAULT FALSE;

    /* Variables para identificar que es Talla (Pivot) y que es Color (Grupo) */
    DECLARE v_id_atributo_pivot VARCHAR(20);  /* Ej: TAL */
    DECLARE v_id_atributo_grupo VARCHAR(20);  /* Ej: CO */

    /* Variable para filtrar si entramos con un SKU especifico (Ej: NEGRO) */
    DECLARE v_valor_grupo_filtro VARCHAR(50) DEFAULT NULL;

    DECLARE v_columnas_dinamicas TEXT;

    /* 1. RESOLUCION DE IDENTIDAD (¿Es Padre o SKU?)
       Antes: 2 sub-queries secuenciales (~100ms cada una con overhead SP).
       Ahora: COALESCE en una sola sentencia. PK seek en fza_articulos
       primero (el caso comun); si devuelve NULL, PK seek en
       fza_articulos_skus. El optimizador evalua el segundo SELECT solo
       cuando el primero da NULL.
       Si el resultado difiere del input literal, era un SKU. */
    SELECT COALESCE(
        (SELECT CODIGO_ART_ART
           FROM fza_articulos
          WHERE CODIGO_ART_ART = p_input
          LIMIT 1),
        (SELECT CODIGO_ART_SKU
           FROM fza_articulos_skus
          WHERE CODIGO_UNIDAD_SKU = p_input
          LIMIT 1)
    ) INTO v_codigo_articulo;

    IF v_codigo_articulo IS NOT NULL AND v_codigo_articulo <> p_input THEN
        SET v_es_sku = TRUE;
    END IF;

    /* 2. ESTRATEGIA DE ATRIBUTOS
       Antes: SET v_id_atributo_pivot = (SELECT ... 4-way JOIN sk -> ask -> av -> va
              WHERE sk.CODIGO_ART_SKU = v_codigo_articulo ORDER BY va.ORDEN_VA DESC LIMIT 1)
              + idem para el grupo. 2 round-trips, 8 JOINs en total.
       Ahora: PK seek sobre fza_articulos para sacar TIPO_VARIACION_ART, y
              index seek sobre fza_variaciones_atributos. Los atributos de
              la variacion estan definidos ahi, no hace falta recorrer SKUs.
              Mismo cambio que metimos en ActualizarColumnasDinamicas. */
    IF v_codigo_articulo IS NOT NULL THEN
        SELECT vat.ID_ATB_VA
          INTO v_id_atributo_pivot
          FROM fza_articulos art
          JOIN fza_variaciones_atributos vat
            ON vat.ID_VAR_VA = art.TIPO_VARIACION_ART
         WHERE art.CODIGO_ART_ART = v_codigo_articulo
         ORDER BY vat.ORDEN_VA DESC
         LIMIT 1;

        SELECT vat.ID_ATB_VA
          INTO v_id_atributo_grupo
          FROM fza_articulos art
          JOIN fza_variaciones_atributos vat
            ON vat.ID_VAR_VA = art.TIPO_VARIACION_ART
         WHERE art.CODIGO_ART_ART = v_codigo_articulo
           AND vat.ID_ATB_VA <> COALESCE(v_id_atributo_pivot, '')
         ORDER BY vat.ORDEN_VA DESC
         LIMIT 1;
    END IF;

    /* 3. SI TENEMOS ESTRUCTURA PARA PIVOTAR... */
    IF v_id_atributo_pivot IS NOT NULL THEN

        /* 3.1 Si es SKU, detectamos el valor del atributo "Grupo".
           Sin cambios — 2 JOINs filtrados por PK del SKU. */
        IF v_es_sku = TRUE AND v_id_atributo_grupo IS NOT NULL THEN
            SET v_valor_grupo_filtro = (
                SELECT av.AV
                  FROM fza_atributos_sku ask
                  JOIN fza_atributos_valores av ON ask.ID_AV_SA = av.ID_AV
                 WHERE ask.CODIGO_UNIDAD_SKU_SA = p_input
                   AND av.ID_VA_AV = v_id_atributo_grupo
                 LIMIT 1
            );
        END IF;

        /* 3.2 Construir columnas dinamicas (S, M, L...).
           Sin cambios — necesitamos los AV REALES presentes en los SKUs del
           articulo, no los del catalogo (un articulo puede no usar todas las
           tallas posibles). Filtrado por CODIGO_ART_SKU (IDX_SKU_ART_ACT) y
           ID_VA_AV (IDX_VAR_AV). */
        SELECT GROUP_CONCAT(DISTINCT
            CONCAT(
                'SUM(CASE WHEN av_p.AV = ''', REPLACE(av.AV, '''', ''''''),
                ''' THEN stk.CANTIDAD_STK ELSE 0 END) AS `', REPLACE(av.AV, '`', '``'), '`'
            )
            ORDER BY av.ID_AV
        ) INTO v_columnas_dinamicas
        FROM fza_articulos_skus sk
        JOIN fza_atributos_sku ask ON sk.CODIGO_UNIDAD_SKU = ask.CODIGO_UNIDAD_SKU_SA
        JOIN fza_atributos_valores av ON ask.ID_AV_SA = av.ID_AV
        WHERE sk.CODIGO_ART_SKU = v_codigo_articulo
          AND av.ID_VA_AV = v_id_atributo_pivot;

        /* 3.3 Construir Query Final — sin cambios. */
        IF v_columnas_dinamicas IS NOT NULL THEN
            SET @sql = CONCAT(
                'SELECT
                    CONCAT(''', v_codigo_articulo, ''',
                        CASE
                            WHEN av_g.AV IS NOT NULL THEN CONCAT(''/'', av_g.AV)
                            ELSE ''''
                        END
                    ) AS Codigo,
                    alm.NOMBRE_ALM_ALM AS Almacen, ',
                    v_columnas_dinamicas, ',
                    SUM(stk.CANTIDAD_STK) AS Total
                 FROM fza_articulos_stockactual stk
                 JOIN fza_almacenes alm ON stk.CODIGO_ALM_STK = alm.CODIGO_ALM_ALM
                 JOIN fza_articulos_skus sk ON stk.CODIGO_UNIDAD_STK = sk.CODIGO_UNIDAD_SKU

                 JOIN fza_atributos_sku ask_p ON sk.CODIGO_UNIDAD_SKU = ask_p.CODIGO_UNIDAD_SKU_SA
                 JOIN fza_atributos_valores av_p ON ask_p.ID_AV_SA = av_p.ID_AV
                    AND av_p.ID_VA_AV = ''', v_id_atributo_pivot, '''

                 LEFT JOIN fza_atributos_sku ask_g ON sk.CODIGO_UNIDAD_SKU = ask_g.CODIGO_UNIDAD_SKU_SA
                 LEFT JOIN fza_atributos_valores av_g ON ask_g.ID_AV_SA = av_g.ID_AV
                    AND av_g.ID_VA_AV = ''', IFNULL(v_id_atributo_grupo, 'xxx'), '''

                 WHERE sk.CODIGO_ART_SKU = ''', v_codigo_articulo, '''
                 ',
                 CASE WHEN v_valor_grupo_filtro IS NOT NULL THEN
                    CONCAT(' AND av_g.AV = ''', REPLACE(v_valor_grupo_filtro, '''', ''''''), ''' ')
                 ELSE '' END, '

                 GROUP BY av_g.AV, alm.NOMBRE_ALM_ALM
                 ORDER BY alm.NOMBRE_ALM_ALM, av_g.AV'
            );

            PREPARE stmt FROM @sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;

        END IF;
    END IF;

    /* CASO B: FALLBACK (Si no tiene atributos complejos o falla el pivotado) */
    IF v_columnas_dinamicas IS NULL THEN
        SELECT stk.CODIGO_UNIDAD_STK as Codigo,
            alm.NOMBRE_ALM_ALM as Almacen,
            SUM(stk.CANTIDAD_STK) as Stock_Total
        FROM fza_articulos_stockactual stk
        JOIN fza_almacenes alm ON stk.CODIGO_ALM_STK = alm.CODIGO_ALM_ALM
        WHERE stk.CODIGO_UNIDAD_STK = p_input
        GROUP BY stk.CODIGO_UNIDAD_STK, alm.NOMBRE_ALM_ALM
        ORDER BY alm.NOMBRE_ALM_ALM;
    END IF;

END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_GET_CAJA_STOCK_PIVOTADO_WITHZ
DROP PROCEDURE IF EXISTS `PRC_GET_CAJA_STOCK_PIVOTADO_WITHZ`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_GET_CAJA_STOCK_PIVOTADO_WITHZ`(IN p_input VARCHAR(50))
BEGIN
    DECLARE v_codigo_articulo VARCHAR(20);
    DECLARE v_es_sku BOOLEAN DEFAULT FALSE;

    /* Variables para las COLUMNAS (Pivot - Ej: Talla) */
    DECLARE v_id_atributo_pivot VARCHAR(20);
    DECLARE v_nombre_atributo_pivot VARCHAR(50);
    DECLARE v_columnas_dinamicas TEXT;

    /* Variables para las FILAS (Desglose - Ej: Color) */
    DECLARE v_id_atributo_fila VARCHAR(20) DEFAULT NULL;
    DECLARE v_nombre_atributo_fila VARCHAR(50) DEFAULT NULL;
    DECLARE v_select_fila TEXT DEFAULT '';
    DECLARE v_join_fila TEXT DEFAULT '';
    DECLARE v_groupby_fila TEXT DEFAULT '';
    DECLARE v_src_select_fila TEXT DEFAULT '';

    DECLARE v_filtros_fijos TEXT DEFAULT '';
    DECLARE v_sql_query TEXT;

    /* 1. IDENTIFICAR ARTICULO O SKU — COALESCE en una sola sentencia. */
    SELECT COALESCE(
        (SELECT CODIGO_ART_ART
           FROM fza_articulos
          WHERE CODIGO_ART_ART = p_input
          LIMIT 1),
        (SELECT CODIGO_ART_SKU
           FROM fza_articulos_skus
          WHERE CODIGO_UNIDAD_SKU = p_input
          LIMIT 1)
    ) INTO v_codigo_articulo;

    IF v_codigo_articulo IS NOT NULL AND v_codigo_articulo <> p_input THEN
        SET v_es_sku = TRUE;
    END IF;

    /* 2. BUSCAR ATRIBUTO PIVOTE (Para las columnas).
       Optimizado: via TIPO_VARIACION_ART (PK seek + index seek). */
    IF v_codigo_articulo IS NOT NULL THEN
        SELECT vat.ID_ATB_VA, vat.NOMBRE_VA
          INTO v_id_atributo_pivot, v_nombre_atributo_pivot
          FROM fza_articulos art
          JOIN fza_variaciones_atributos vat
            ON vat.ID_VAR_VA = art.TIPO_VARIACION_ART
         WHERE art.CODIGO_ART_ART = v_codigo_articulo
         ORDER BY vat.ORDEN_VA DESC
         LIMIT 1;
    END IF;

    /* 2.1 BUSCAR ATRIBUTO DE FILA (Ej: Color) SI ES UN ARTICULO GENERICO.
       Mantenemos ORDER BY ORDEN_VA ASC (la semantica original del SP _WITHZ
       difiere del PIVOTADO sin Z, que usaba DESC excluyendo el pivot).
       Para variaciones de 2 atributos da el mismo resultado. */
    IF v_codigo_articulo IS NOT NULL AND v_es_sku = FALSE THEN
        SELECT vat.ID_ATB_VA, vat.NOMBRE_VA
          INTO v_id_atributo_fila, v_nombre_atributo_fila
          FROM fza_articulos art
          JOIN fza_variaciones_atributos vat
            ON vat.ID_VAR_VA = art.TIPO_VARIACION_ART
         WHERE art.CODIGO_ART_ART = v_codigo_articulo
           AND vat.ID_ATB_VA <> v_id_atributo_pivot
         ORDER BY vat.ORDEN_VA ASC
         LIMIT 1;
    END IF;

    /* 3. CONSTRUCCION DE LA CONSULTA */
    IF v_id_atributo_pivot IS NOT NULL THEN

        /* Preparamos los textos dinamicos para desglosar el Color en filas */
        IF v_id_atributo_fila IS NOT NULL THEN
            SET v_select_fila = CONCAT(', COALESCE(src.VALOR_FILA, ''-'') AS `', v_nombre_atributo_fila, '`');
            SET v_src_select_fila = ', av_fila.AV AS VALOR_FILA';
            SET v_join_fila = CONCAT(' LEFT JOIN fza_atributos_sku ask_fila ON sk.CODIGO_UNIDAD_SKU = ask_fila.CODIGO_UNIDAD_SKU_SA LEFT JOIN fza_atributos_valores av_fila ON ask_fila.ID_AV_SA = av_fila.ID_AV AND av_fila.ID_VA_AV = ''', v_id_atributo_fila, ''' ');
            SET v_groupby_fila = ', src.VALOR_FILA';
        END IF;

        /* Generamos columnas dinamicas apuntando a la subconsulta 'src'.
           Sin cambios — necesita los AV reales de los SKUs. */
        SELECT GROUP_CONCAT(DISTINCT
            CONCAT(
                'SUM(CASE WHEN src.AV = ''', av.AV,
                ''' THEN src.CANTIDAD_STK ELSE 0 END) AS `', av.AV, '`'
            )
            ORDER BY av.ID_AV
        ) INTO v_columnas_dinamicas
        FROM fza_articulos_skus sk
        JOIN fza_atributos_sku ask ON sk.CODIGO_UNIDAD_SKU = ask.CODIGO_UNIDAD_SKU_SA
        JOIN fza_atributos_valores av ON ask.ID_AV_SA = av.ID_AV
        WHERE sk.CODIGO_ART_SKU = v_codigo_articulo
          AND av.ID_VA_AV = v_id_atributo_pivot;

        /* Filtros SKU (Solo aplica si buscas un SKU especifico) */
        IF v_es_sku = TRUE THEN
            SELECT GROUP_CONCAT(
                CONCAT(
                    ' AND EXISTS (SELECT 1 FROM fza_atributos_sku f_ask ',
                    ' JOIN fza_atributos_valores f_av ON f_ask.ID_AV_SA = f_av.ID_AV ',
                    ' WHERE f_ask.CODIGO_UNIDAD_SKU_SA = sk.CODIGO_UNIDAD_SKU ',
                    ' AND f_av.ID_VA_AV = ''', av.ID_VA_AV, ''' ',
                    ' AND f_av.AV = ''', av.AV, ''') '
                ) SEPARATOR ' '
            ) INTO v_filtros_fijos
            FROM fza_atributos_sku ask
            JOIN fza_atributos_valores av ON ask.ID_AV_SA = av.ID_AV
            WHERE ask.CODIGO_UNIDAD_SKU_SA = p_input
              AND av.ID_VA_AV <> v_id_atributo_pivot;
        END IF;

        IF v_filtros_fijos IS NULL THEN SET v_filtros_fijos = ''; END IF;

        /* Si el articulo tiene atributo pivote definido pero todavia no
           tiene SKUs cargados, GROUP_CONCAT devuelve NULL. CONCAT con un
           argumento NULL devuelve NULL, y PREPARE stmt FROM NULL revienta
           con "near 'NULL' at line 1". Caemos al fallback. */
        IF v_columnas_dinamicas IS NOT NULL THEN

            /* QUERY FINAL: Almacen + Atributo Fila (Color) + Columnas Dinamicas (Talla) */
            SET @sql = CONCAT(
                'SELECT
                    alm.NOMBRE_ALM_ALM AS Almacen',
                    v_select_fila, ', ',
                    v_columnas_dinamicas, ',
                    COALESCE(SUM(src.CANTIDAD_STK), 0) AS Total
                 FROM fza_almacenes alm
                 LEFT JOIN (
                    SELECT stk.CODIGO_ALM_STK, av.AV, stk.CANTIDAD_STK', v_src_select_fila, '
                    FROM fza_articulos_stockactual stk
                    JOIN fza_articulos_skus sk ON stk.CODIGO_UNIDAD_STK = sk.CODIGO_UNIDAD_SKU
                    JOIN fza_atributos_sku ask ON sk.CODIGO_UNIDAD_SKU = ask.CODIGO_UNIDAD_SKU_SA
                    JOIN fza_atributos_valores av ON ask.ID_AV_SA = av.ID_AV',
                    v_join_fila, '
                    WHERE sk.CODIGO_ART_SKU = ''', v_codigo_articulo, '''
                      AND av.ID_VA_AV = ''', v_id_atributo_pivot, ''' ',
                      v_filtros_fijos, '
                 ) src ON alm.CODIGO_ALM_ALM = src.CODIGO_ALM_STK
                 WHERE alm.ESACTIVO_ALM = ''S''
                 GROUP BY alm.NOMBRE_ALM_ALM', v_groupby_fila, '
                 ORDER BY alm.NOMBRE_ALM_ALM', v_groupby_fila
            );

            PREPARE stmt FROM @sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;

        END IF;

    END IF;

    /* Fallback: articulo simple, articulo sin TIPO_VARIACION_ART o
       articulo con pivote pero sin SKUs. Devuelve fila por almacen
       con stock total (0 si no hay nada todavia). */
    IF v_id_atributo_pivot IS NULL OR v_columnas_dinamicas IS NULL THEN
        SELECT
            alm.NOMBRE_ALM_ALM as Almacen,
            COALESCE(SUM(stk.CANTIDAD_STK), 0) as `Stock Total`
        FROM fza_almacenes alm
        LEFT JOIN fza_articulos_stockactual stk
            ON alm.CODIGO_ALM_ALM = stk.CODIGO_ALM_STK
            AND stk.CODIGO_UNIDAD_STK = p_input
        WHERE alm.ESACTIVO_ALM = 'S'
        GROUP BY alm.NOMBRE_ALM_ALM
        ORDER BY alm.NOMBRE_ALM_ALM;
    END IF;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_GET_CREAR_VALOR
DROP PROCEDURE IF EXISTS `PRC_GET_CREAR_VALOR`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_GET_CREAR_VALOR`(IN `p_id_va` VARCHAR(20),       /* Tipo de Variación (ej: 'CO') */
    IN `p_valor` VARCHAR(100),      /* El valor escrito (ej: 'VERDE LIMA') */
    IN `p_usuario` VARCHAR(100),    /* Usuario que hace la acción */
    OUT `p_id_resultado` INT)
BEGIN
    /* 1. Intentamos buscar el ID si ya existe */
    SELECT `ID_AV` INTO p_id_resultado
    FROM `fza_atributos_valores`
    WHERE `ID_VA_AV` = p_id_va 
      AND UPPER(`AV`) = UPPER(p_valor) /* Comparamos ignorando mayúsculas */
    LIMIT 1;

    /* 2. Si p_id_resultado sigue siendo NULL, significa que no existe. LO CREAMOS. */
    IF p_id_resultado IS NULL THEN
        INSERT INTO `fza_atributos_valores` (
            `ID_VA_AV`, 
            `AV`, 
            `USUARIO_ALTA`, 
            `USUARIO_MODIF`, 
            `INSTANTE_ALTA`
        ) VALUES (
            p_id_va, 
            p_valor, 
            p_usuario, 
            p_usuario, 
            NOW()
        );
        
        /* Obtenemos el ID que se acaba de generar */
        SET p_id_resultado = LAST_INSERT_ID();
    END IF;

    /* Al final, p_id_resultado siempre tiene un número válido (viejo o nuevo). */
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_GET_DATA_ARTICULO
DROP PROCEDURE IF EXISTS `PRC_GET_DATA_ARTICULO`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_GET_DATA_ARTICULO`(
    IN pidcodarticulo varchar(200),
    OUT pidnomarticulo varchar(1000),
    OUT ptipoiva varchar(2)
)
BEGIN
    /* Declaramos una bandera para saber si lo encuentra o no */
    DECLARE v_found INT DEFAULT 1;
    
    /* Si el SELECT INTO no encuentra filas, baja esta bandera en vez de dar error */
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_found = 0;

    /* Hacemos la consulta UNA SOLA VEZ */
    SELECT DESCRIPCION_ART, TIPO_IVA_ART 
    INTO pidnomarticulo, ptipoiva 
    FROM fza_articulos 
    WHERE CODIGO_ART_ART = pidcodarticulo;

    /* Verificamos si la bandera cayó */
    IF v_found = 0 THEN
        SET pidnomarticulo = 'NO EXISTE';
        SET ptipoiva = 'N';
    END IF;
    
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_GET_DATA_CLIENTE
DROP PROCEDURE IF EXISTS `PRC_GET_DATA_CLIENTE`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_GET_DATA_CLIENTE`(
    IN  pCODIGO_CLIENTE                    varchar(10),
    OUT pRAZONSOCIAL_CLIENTE               varchar(200),
    OUT pNIF_CLIENTE                       varchar(50),
    OUT pCODIGO_ZONA_IVA_CLIENTE           int,
    OUT pMOVIL_CLIENTE                     varchar(40),
    OUT pESIVA_RECARGO_CLIENTE             varchar(1),
    OUT pESRETENCIONES_CLIENTE             varchar(1),
    OUT pESIVA_EXENTO_CLIENTE              varchar(1),
    OUT pESINTRACOMUNITARIO_CLIENTE        varchar(1),
    OUT pESREGIMENESPECIALAGRICOLA_CLIENTE varchar(1),
    OUT pEMAIL_CLIENTE                     varchar(200),
    OUT pDIRECCION1_CLIENTE                varchar(200),
    OUT pDIRECCION2_CLIENTE                varchar(200),
    OUT pPOBLACION_CLIENTE                 varchar(200),
    OUT pPROVINCIA_CLIENTE                 varchar(200),
    OUT pCPOSTAL_CLIENTE                   varchar(15),
    OUT pTARIFA_ARTICULO_CLIENTE           varchar(10),
    OUT pTEXTO_LEGAL_FACTURA_CLIENTE       varchar(1000),
    OUT pPAIS_CLIENTE                      varchar(150),
    OUT pCOD_PAIS_CLIENTE                  varchar(150)
)
BEGIN

    /* Comprobamos si el cliente existe (Usamos SELECT 1 por rendimiento) */
    IF ( EXISTS( SELECT 1
                 FROM fza_clientes
                 WHERE CODIGO_CLI_CLI = pCODIGO_CLIENTE ) ) THEN
                 
        /* Si existe, metemos los datos en las variables OUT */
        SELECT  RAZON_SOCIAL_CLI,
                NIF_CLI,
                CODIGO_ZONA_IVA_CLIENTE,
                ESIVA_RECARGO_CLI,
                ESRETENCIONES_CLI,
                ESIVA_EXENTO_CLI,
                ESINTRACOMUNITARIO_CLI,
                ESREGIMENESPECIALAGRICOLA_CLI,
                MOVIL_CLI,
                EMAIL_CLI,
                DIRECCION1_CLI,
                DIRECCION2_CLI,
                POBLACION_CLI,
                PROVINCIA_CLI,
                CODIGO_POSTAL_CLI,
                TARIFA_ARTICULO_CLI,
                TEXTO_LEGAL_FACTURA_CLI,
                NOMBRE_PAI_CLI,
                CODIGO_PAI_CLI 
        INTO    pRAZONSOCIAL_CLIENTE,
                pNIF_CLIENTE,
                pCODIGO_ZONA_IVA_CLIENTE,
                pESIVA_RECARGO_CLIENTE,
                pESRETENCIONES_CLIENTE,
                pESIVA_EXENTO_CLIENTE,
                pESINTRACOMUNITARIO_CLIENTE,
                pESREGIMENESPECIALAGRICOLA_CLIENTE,
                pMOVIL_CLIENTE,
                pEMAIL_CLIENTE,
                pDIRECCION1_CLIENTE,
                pDIRECCION2_CLIENTE,
                pPOBLACION_CLIENTE,
                pPROVINCIA_CLIENTE,
                pCPOSTAL_CLIENTE,
                pTARIFA_ARTICULO_CLIENTE,
                pTEXTO_LEGAL_FACTURA_CLIENTE,
                pPAIS_CLIENTE,
                pCOD_PAIS_CLIENTE
        FROM fza_clientes
        WHERE CODIGO_CLI_CLI = pCODIGO_CLIENTE;
        
    ELSE
    
        /* Si no existe, devolvemos un aviso */
        SET pRAZONSOCIAL_CLIENTE = 'CLIENTE NO ENCONTRADO';
        
    END IF;

END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_GET_IVA_ZONA_FECHA
DROP PROCEDURE IF EXISTS `PRC_GET_IVA_ZONA_FECHA`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_GET_IVA_ZONA_FECHA`(IN `pFECHA` DATE, 
                                       IN `pZONA` INT, 
                                       OUT `pRESUL` INT, 
                                       OUT `pEXENTO_IVA` DECIMAL(18,6), 
                                       OUT `pEXENTO_RE_IVA` DECIMAL(18,6), 
                                       OUT `pNORMAL_IVA` DECIMAL(18,6), 
                                       OUT `pNORMAL_RE_IVA` DECIMAL(18,6), 
                                       OUT `pREDUCIDO_IVA` DECIMAL(18,6), 
                                       OUT `pREDUCIDO_RE_IVA` DECIMAL(18,6), 
                                       OUT `pSUPERREDUCIDO_IVA` DECIMAL(18,6), 
                                       OUT `pSUPERREDUCIDO_RE_IVA` DECIMAL(18,6))
BEGIN
    IF( EXISTS(
             SELECT *
               FROM fza_ivas
              WHERE FECHA_DESDE_IVA >=  FECHA
                AND (FECHA_HASTA_IVA <= FECHA 
                 OR FECHA_HASTA_IVA IS NULL)
                AND  IVA_IVAGRP = pZONA)) THEN
  SELECT `PORCENTAJE_EXENTO_IVA` ,
         `PORCENTAJE_EXENTO_RE_IVA`,
         `PORCENTAJE_NORMAL_IVA` ,
         `PORCENTAJE_NORMAL_RE_IVA`,
         `PORCENTAJE_REDUCIDO_IVA` ,
         `PORCENTAJE_REDUCIDO_RE_IVA` ,
         `PORCENTAJE_SUPERREDUCIDO_IVA` ,
         `PORCENTAJE_SUPERREDUCIDO_RE_IVA`
    INTO
         pEXENTO_IVA,
         pEXENTO_RE_IVA,
         pNORMAL_IVA ,
         pNORMAL_RE_IVA,
         pREDUCIDO_IVA,
         pREDUCIDO_RE_IVA ,
         pSUPERREDUCIDO_IVA,
         pSUPERREDUCIDO_RE_IVA
    FROM fza_ivas
   WHERE (FECHA_DESDE_IVA >=  FECHA
     AND (FECHA_HASTA_IVA <= FECHA OR 
          FECHA_HASTA_IVA IS NULL))
     AND CODIGO_ZONA_IVA_IVAZON = pZONA;
ELSE
    SELECT `PORCENTAJE_EXENTO_IVA` ,
           `PORCENTAJE_EXENTO_RE_IVA`,
           `PORCENTAJE_NORMAL_IVA` ,
           `PORCENTAJE_NORMAL_RE_IVA`,
           `PORCENTAJE_REDUCIDO_IVA` ,
           `PORCENTAJE_REDUCIDO_RE_IVA` ,
           `PORCENTAJE_SUPERREDUCIDO_IVA` ,
           `PORCENTAJE_SUPERREDUCIDO_RE_IVA`
       INTO
            pEXENTO_IVA,
            pEXENTO_RE_IVA,
            pNORMAL_IVA ,
            pNORMAL_RE_IVA,
            pREDUCIDO_IVA,
            pREDUCIDO_RE_IVA ,
            pSUPERREDUCIDO_IVA,
            pSUPERREDUCIDO_RE_IVA
      FROM  fza_ivas
     WHERE (FECHA_DESDE_IVA >=  FECHA
       AND FECHA_HASTA_IVA IS NULL )
       AND CODIGO_ZONA_IVA_IVAZON = pZONA;
  END IF;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_GET_NEXT_CONT
DROP PROCEDURE IF EXISTS `PRC_GET_NEXT_CONT`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_GET_NEXT_CONT`(
    IN  pTipoDoc       varchar(2), 
    IN  pUSUARIO_MODIF varchar(100),
    OUT pcont          varchar(20)
)
BEGIN
    DECLARE pPADD bigint;
    DECLARE pEMPRESA_CONTADOR varchar(10) DEFAULT '-';
    DECLARE v_NextValue BIGINT; /* Variable añadida para guardar el valor atómico */

    /* Añadido: Manejador de excepciones con etiqueta 'kk:' */
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    kk: BEGIN 
        ROLLBACK; 
        RESIGNAL; 
    END kk;
    
    START TRANSACTION;

    IF (pEMPRESA_CONTADOR = '' OR pEMPRESA_CONTADOR IS NULL) THEN 
        SET pEMPRESA_CONTADOR = '-'; 
    END IF;

    /* Si no existe el contador, lo creamos (lógica original intacta) */
    IF NOT EXISTS (
        SELECT 1
        FROM fza_contadores
        WHERE TIPO_DOC_CON = pTipoDoc
          AND EMPRESA_CON = pEMPRESA_CONTADOR
    ) THEN
        INSERT INTO fza_contadores (
            TIPO_DOC_CON, 
            SERIE_CON,
            EMPRESA_CON,   
            CON, 
            DEFAULT_CON,
            NUM_DIGITOS_CON,
            INSTANTE_ALTA, 
            USUARIO_ALTA,
            USUARIO_MODIF
        ) VALUES (
            pTipoDoc, 
            '-', 
            pEMPRESA_CONTADOR,
            1, 
            'S', 
            3,
            CURRENT_TIMESTAMP,
            pUSUARIO_MODIF, 
            pUSUARIO_MODIF
        );
    END IF;

    /* Obtenemos el número de dígitos / padding (lógica original intacta) */
    SET pPADD = (
        SELECT NUM_DIGITOS_CON 
        FROM fza_contadores 
        WHERE TIPO_DOC_CON = pTipoDoc 
          AND EMPRESA_CON = pEMPRESA_CONTADOR
          AND DEFAULT_CON = 'S' 
        LIMIT 1
    );
     
    /* Modificado: Sumamos 1 al contador usando LAST_INSERT_ID de forma atómica */
    UPDATE fza_contadores 
    SET CON = LAST_INSERT_ID(CON + 1),
        USUARIO_MODIF = pUSUARIO_MODIF
    WHERE TIPO_DOC_CON = pTipoDoc
      AND EMPRESA_CON = pEMPRESA_CONTADOR;            
      
    /* Modificado: Guardamos el valor exacto bloqueado para este usuario y le restamos 1 como en tu lógica original */
    SET v_NextValue = LAST_INSERT_ID() - 1;

    /* Modificado: Devolvemos el valor con el LPAD usando la variable segura */
    SET pcont = LPAD(v_NextValue, pPADD, '0');

    COMMIT;
    
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_GET_NEXT_CONT_FACT_SERIE
DROP PROCEDURE IF EXISTS `PRC_GET_NEXT_CONT_FACT_SERIE`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_GET_NEXT_CONT_FACT_SERIE`(
    IN pserie VARCHAR(12),
    IN pTipoDoc VARCHAR(2),
    IN pEMPRESA_CONTADOR VARCHAR(10),
    IN pUSUARIOMODIF VARCHAR(100),
    OUT pcont VARCHAR(12)
)
BEGIN
    DECLARE pNUMDIGIT INT;
    DECLARE v_NextValue BIGINT;
    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    ManejoError: BEGIN ROLLBACK; RESIGNAL; END ManejoError;

    START TRANSACTION;

    IF (pEMPRESA_CONTADOR = '' OR pEMPRESA_CONTADOR IS NULL) THEN
        SET pEMPRESA_CONTADOR = '-';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM fza_contadores WHERE TIPO_DOC_CON = pTipoDoc AND EMPRESA_CON = pEMPRESA_CONTADOR AND SERIE_CON = pserie) THEN
        INSERT INTO fza_contadores (
            TIPO_DOC_CON, SERIE_CON, CON, EMPRESA_CON, DEFAULT_CON, NUM_DIGITOS_CON, INSTANTE_ALTA, USUARIO_ALTA, USUARIO_MODIF
        ) VALUES (
            pTipoDoc, pserie, 1, pEMPRESA_CONTADOR, 'N', 6, CURRENT_TIMESTAMP, pUSUARIOMODIF, pUSUARIOMODIF
        );
    END IF;

    /* LA MAGIA: Incrementamos y guardamos en memoria el valor EXACTO para este usuario */
    UPDATE fza_contadores 
    SET CON = LAST_INSERT_ID(CON + 1), 
        USUARIO_MODIF = pUSUARIOMODIF 
    WHERE SERIE_CON = pserie AND EMPRESA_CON = pEMPRESA_CONTADOR AND TIPO_DOC_CON = pTipoDoc;

    /* Obtenemos el valor seguro reservado para esta sesión (le restamos 1 porque la app necesita el número pre-incremento) */
    SET v_NextValue = LAST_INSERT_ID() - 1;

    SELECT NUM_DIGITOS_CON INTO pNUMDIGIT 
    FROM fza_contadores 
    WHERE SERIE_CON = pserie AND TIPO_DOC_CON = pTipoDoc AND EMPRESA_CON = pEMPRESA_CONTADOR LIMIT 1;

    IF (pNUMDIGIT IS NOT NULL AND pNUMDIGIT > 0) THEN
        SET pcont = LPAD(v_NextValue, pNUMDIGIT, '0');
    ELSE
        SET pcont = CAST(v_NextValue AS CHAR);
    END IF;

    COMMIT;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_GET_NEXT_OP_CAJA
DROP PROCEDURE IF EXISTS `PRC_GET_NEXT_OP_CAJA`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_GET_NEXT_OP_CAJA`(
    IN  pEmpresa  VARCHAR(10),
    IN  pAlmacen  VARCHAR(10),
    IN  pCaja     VARCHAR(10),
    IN  pUsuario  VARCHAR(100),
    OUT pSerie    VARCHAR(12),
    OUT pcont     VARCHAR(20)
)
BEGIN    
    DECLARE pPADD    BIGINT;
    DECLARE vSerie   VARCHAR(12);

    START TRANSACTION;

        /* 1. Obtener serie vigente */        
        SELECT EMPSER          
          INTO vSerie          
          FROM fza_empresas_series         
         WHERE TIPO_DOC_EMPSER        = 'OV'           
           AND CODIGO_EMP_EMPSER = pEmpresa           
           AND CODIGO_ALM_EMPSER = pAlmacen           
           AND CODIGO_CAJA_EMPSER    = pCaja           
           AND FECHA_DESDE_EMPSER   <= CURDATE()
           AND (FECHA_HASTA_EMPSER  >= CURDATE() OR FECHA_HASTA_EMPSER IS NULL)
         LIMIT 1;

        /* 2. Crear serie si no existe (Sustituye el bloque de Error) */
        IF vSerie IS NULL THEN            
            SET vSerie = 'OV'; /* Nombre por defecto de la nueva serie */
            
            INSERT INTO fza_empresas_series (
                TIPO_DOC_EMPSER,
                CODIGO_EMP_EMPSER,
                CODIGO_ALM_EMPSER,
                CODIGO_CAJA_EMPSER,
                EMPSER,
                FECHA_DESDE_EMPSER,
                FECHA_HASTA_EMPSER
                /* Descomenta y ajusta si tu tabla tiene columnas de auditoría:
                , INSTANTE_ALTA,
                USUARIO_ALTA
                */
            ) VALUES (
                'OV',
                pEmpresa,
                pAlmacen,
                pCaja,
                vSerie,
                CURDATE(),
                NULL
                /*
                , CURRENT_TIMESTAMP,
                pUsuario
                */
            );
        END IF;

        /* 3. Crear fila de contador si no existe */        
        IF NOT EXISTS (
            SELECT 1              
              FROM fza_contadores             
             WHERE TIPO_DOC_CON = 'OV'               
               AND EMPRESA_CON = pEmpresa               
               AND SERIE_CON   = vSerie        
        ) THEN            
            INSERT INTO fza_contadores (
                TIPO_DOC_CON,
                EMPRESA_CON,
                SERIE_CON,
                CON,
                NUM_DIGITOS_CON,
                ESACTIVO_CON,
                DEFAULT_CON,
                INSTANTE_ALTA,
                USUARIO_ALTA,
                USUARIO_MODIF)
            VALUES (
                'OV',
                pEmpresa,
                vSerie,
                1,
                8,
                'S',
                'S',
                CURRENT_TIMESTAMP,
                pUsuario,
                pUsuario);
        END IF;

        /* 4. Bloquear fila y leer dígitos */        
        SELECT NUM_DIGITOS_CON          
          INTO pPADD          
          FROM fza_contadores         
         WHERE TIPO_DOC_CON = 'OV'           
           AND EMPRESA_CON = pEmpresa           
           AND SERIE_CON   = vSerie         
         LIMIT 1           
           FOR UPDATE;

        /* 5. Incrementar */        
        UPDATE fza_contadores           
           SET CON = CON + 1,
               USUARIO_MODIF      = pUsuario         
         WHERE TIPO_DOC_CON  = 'OV'           
           AND EMPRESA_CON  = pEmpresa           
           AND SERIE_CON    = vSerie;

        /* 6. Devolver serie y número formateado */        
        SET pSerie := vSerie;

        SELECT LPAD(CON - 1, pPADD, '0')
          INTO pcont          
          FROM fza_contadores         
         WHERE TIPO_DOC_CON = 'OV'           
           AND EMPRESA_CON = pEmpresa           
           AND SERIE_CON   = vSerie         
         LIMIT 1;

    COMMIT;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_GET_NUMEROS_A_LETRAS
DROP PROCEDURE IF EXISTS `PRC_GET_NUMEROS_A_LETRAS`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_GET_NUMEROS_A_LETRAS`(
    IN NUMERO DECIMAL(12,2), 
    OUT pResul varchar(200)
)
BEGIN
    DECLARE MILLARES INT;
    DECLARE CENTENAS INT;
    DECLARE CENTIMOS INT;
    DECLARE CENTIMO_AUX VARCHAR(200);
    DECLARE CENTIMO_AUX_CON VARCHAR(200);
    DECLARE EN_LETRAS VARCHAR(200);
    DECLARE ENTERO INT;
    DECLARE AUX VARCHAR(15);
    DECLARE INTER VARCHAR(200);
    
    SET EN_LETRAS = '';
    SET CENTIMO_AUX_CON = '';
    SET ENTERO = TRUNCATE(NUMERO, 0);
    SET MILLARES = TRUNCATE(ENTERO / 1000, 0);
    SET CENTENAS = ENTERO MOD 1000;
    SET CENTIMOS = (TRUNCATE(NUMERO, 2) * 100) MOD 100;
    
    /* Procesar Millares */
    IF (MILLARES = 1) THEN
        SET EN_LETRAS = 'MIL ';
    ELSE 
        IF (MILLARES > 0) THEN
            CALL PRC_GET_NUMERO_MENOR_MIL(MILLARES, INTER); 
            SET EN_LETRAS = CONCAT(EN_LETRAS, INTER, 'MIL ');
            SET EN_LETRAS = REPLACE(EN_LETRAS, 'UNO ', 'UN ');
        END IF;
    END IF;
    
    /* Procesar Centenas */
    IF ((CENTENAS > 0) OR ((ENTERO = 0) AND (CENTIMOS = 0))) THEN
        CALL PRC_GET_NUMERO_MENOR_MIL(CENTENAS, INTER);
        SET EN_LETRAS = CONCAT(EN_LETRAS, INTER);            
    END IF;
    
    /* Procesar Céntimos */
    IF (CENTIMOS > 0) THEN
        
        IF (CENTIMOS = 1) THEN
            SET AUX = 'CÉNTIMO ';
        ELSE
            SET AUX = 'CÉNTIMOS ';
        END IF;    
        
        CALL PRC_GET_NUMERO_MENOR_MIL(CENTIMOS, INTER);
        SET CENTIMO_AUX = INTER;
        SET CENTIMO_AUX = REPLACE(CENTIMO_AUX, 'UNO ', 'UN '); 
        
        IF (ENTERO <> 0) THEN 
            SET CENTIMO_AUX_CON = 'CON '; 
        END IF;
        
        SET EN_LETRAS = CONCAT(EN_LETRAS, CENTIMO_AUX_CON, CENTIMO_AUX, AUX);
        
    END IF;
    
    SET pResul = EN_LETRAS;
    
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_GET_NUMERO_MENOR_MIL
DROP PROCEDURE IF EXISTS `PRC_GET_NUMERO_MENOR_MIL`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_GET_NUMERO_MENOR_MIL`(IN NUMERO DECIMAL(4), OUT pResul varchar(100))
BEGIN
       DECLARE CENTENAS INT;
       DECLARE DECENAS INT;
       DECLARE UNIDADES INT;
       DECLARE EN_LETRAS VARCHAR(100);
       DECLARE UNIR VARCHAR(2);
			 SET EN_LETRAS = '';
        IF (NUMERO = 100) THEN
            SET pResul = ('CIEN ');
        ELSEIF NUMERO = 0 THEN
            SET pResul = ('CERO ');
        ELSEIF NUMERO = 1 THEN
            SET pResul = ('UNO ');
        ELSE
            SET CENTENAS = TRUNCATE(NUMERO / 100,0);
            SET DECENAS  = TRUNCATE((NUMERO MOD 100)/10,0);
            SET UNIDADES = NUMERO MOD 10;
            SET UNIR = 'Y ';
            
						IF CENTENAS = 1 THEN
                SET EN_LETRAS = 'CIENTO ';
            ELSEIF CENTENAS = 2 THEN
                SET EN_LETRAS = 'DOSCIENTOS ';
            ELSEIF CENTENAS = 3 THEN
                SET EN_LETRAS = 'TRESCIENTOS ';
            ELSEIF CENTENAS = 4 THEN
                SET EN_LETRAS = 'CUATROCIENTOS ';
            ELSEIF CENTENAS = 5 THEN
                SET EN_LETRAS = 'QUINIENTOS ';
            ELSEIF CENTENAS = 6 THEN
                SET EN_LETRAS = 'SEISCIENTOS ';
            ELSEIF CENTENAS = 7 THEN
                SET EN_LETRAS = 'SETECIENTOS ';
            ELSEIF CENTENAS = 8 THEN
                SET EN_LETRAS = 'OCHOCIENTOS ';
            ELSEIF CENTENAS = 9 THEN
                SET EN_LETRAS = 'NOVECIENTOS ';
            END IF;
            
						IF DECENAS = 3 THEN
                SET EN_LETRAS = CONCAT(EN_LETRAS , 'TREINTA ');
            ELSEIF DECENAS = 4 THEN
                SET EN_LETRAS = CONCAT(EN_LETRAS , 'CUARENTA ');
            ELSEIF DECENAS = 5 THEN
                SET EN_LETRAS = CONCAT(EN_LETRAS , 'CINCUENTA ');
            ELSEIF DECENAS = 6 THEN
                SET EN_LETRAS = CONCAT(EN_LETRAS , 'SESENTA ');
            ELSEIF DECENAS = 7 THEN
                SET EN_LETRAS = CONCAT(EN_LETRAS , 'SETENTA ');
            ELSEIF DECENAS = 8 THEN
                SET EN_LETRAS = CONCAT(EN_LETRAS , 'OCHENTA ');
            ELSEIF DECENAS = 9 THEN
                SET EN_LETRAS = CONCAT(EN_LETRAS , 'NOVENTA ');
            ELSEIF DECENAS = 1 THEN
                IF UNIDADES < 6 THEN
                    IF UNIDADES = 0 THEN
                        SET EN_LETRAS = CONCAT(EN_LETRAS , 'DIEZ ');
                    ELSEIF UNIDADES = 1 THEN
                        SET EN_LETRAS = CONCAT(EN_LETRAS , 'ONCE ');
                    ELSEIF UNIDADES = 2 THEN
                        SET EN_LETRAS = CONCAT(EN_LETRAS , 'DOCE ');
                    ELSEIF UNIDADES = 3 THEN
                        SET EN_LETRAS = CONCAT(EN_LETRAS , 'TRECE ');
                    ELSEIF UNIDADES = 4 THEN
                        SET EN_LETRAS = CONCAT(EN_LETRAS , 'CATORCE ');
                    ELSEIF UNIDADES = 5 THEN
                        SET EN_LETRAS = CONCAT(EN_LETRAS , 'QUINCE ');
                    END IF;
                    SET UNIDADES = 0;
                ELSE
                    SET EN_LETRAS = CONCAT(EN_LETRAS, 'DIECI');
                    SET UNIR = '';
                END IF;
            ELSEIF (DECENAS = 2) THEN
                IF (UNIDADES = 0) THEN
                    SET EN_LETRAS = CONCAT(EN_LETRAS, 'VEINTE ');
                ELSE
                    SET EN_LETRAS = CONCAT(EN_LETRAS, 'VEINTI');
                END IF;
                SET UNIR = '';
            ELSEIF (DECENAS = 0) THEN
                SET UNIR = '';
            END IF;
						
            IF (UNIDADES = 1) THEN
                SET EN_LETRAS = CONCAT(EN_LETRAS, UNIR, 'UNO ');
            ELSEIF UNIDADES = 2 THEN
                SET EN_LETRAS = CONCAT(EN_LETRAS, UNIR, 'DOS ');
            ELSEIF UNIDADES = 3 THEN
                SET EN_LETRAS = CONCAT(EN_LETRAS, UNIR, 'TRES ');
            ELSEIF UNIDADES = 4 THEN
                SET EN_LETRAS = CONCAT(EN_LETRAS, UNIR, 'CUATRO ');
            ELSEIF UNIDADES = 5 THEN
                SET EN_LETRAS = CONCAT(EN_LETRAS, UNIR, 'CINCO ');
            ELSEIF UNIDADES = 6 THEN
                SET EN_LETRAS = CONCAT(EN_LETRAS, UNIR, 'SEIS ');
            ELSEIF UNIDADES = 7 THEN
                SET EN_LETRAS = CONCAT(EN_LETRAS, UNIR, 'SIETE ');
            ELSEIF UNIDADES = 8 THEN
                SET EN_LETRAS = CONCAT(EN_LETRAS, UNIR, 'OCHO ');
            ELSEIF UNIDADES = 9 THEN
                SET EN_LETRAS = CONCAT(EN_LETRAS , UNIR , 'NUEVE ');
            END IF;
        END IF;
        SET pResul = EN_LETRAS;
    END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_PED_CREAR_ALBARAN_FIN
DROP PROCEDURE IF EXISTS `PRC_PED_CREAR_ALBARAN_FIN`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_PED_CREAR_ALBARAN_FIN`(
  IN p_NUMERO_ALB varchar(20),
  IN p_SERIE_ALB  varchar(20),
  IN p_NUMERO_PED varchar(20),
  IN p_SERIE_PED  varchar(20),
  IN p_USUARIO    varchar(100)
)
BEGIN
  DECLARE v_total_base    decimal(18,6) DEFAULT 0;
  DECLARE v_total_iva     decimal(18,6) DEFAULT 0;
  DECLARE v_pendientes    int DEFAULT 0;

  SELECT
    IFNULL(SUM(`CANTIDAD_ALBLIN` * `PRECIO_VENTA_SIVA_ARTICULO_ALBLIN`), 0),
    IFNULL(SUM(`CANTIDAD_ALBLIN` * (`PRECIO_VENTA_CIVA_ARTICULO_ALBLIN` -
                                    `PRECIO_VENTA_SIVA_ARTICULO_ALBLIN`)), 0)
    INTO v_total_base, v_total_iva
    FROM `fza_albaranes_lineas`
   WHERE `NUMERO_ALB_ALBLIN` = p_NUMERO_ALB
     AND `SERIE_ALB_ALBLIN`  = p_SERIE_ALB;

  UPDATE `fza_albaranes`
     SET `TOTAL_BASES_ALB`     = v_total_base,
         `TOTAL_IMPUESTOS_ALB` = v_total_iva,
         `TOTAL_LIQUIDO_ALB`   = v_total_base + v_total_iva,
         `INSTANTE_MODIF`      = NOW(),
         `USUARIO_MODIF`       = p_USUARIO
   WHERE `NUMERO_ALB` = p_NUMERO_ALB
     AND `SERIE_ALB`  = p_SERIE_ALB;

  SELECT COUNT(*) INTO v_pendientes
    FROM `fza_pedidos_lineas`
   WHERE `NUMERO_PED_PEDLIN` = p_NUMERO_PED
     AND `SERIE_PED_PEDLIN`  = p_SERIE_PED
     AND IFNULL(`ESENTREGADA_PEDLIN`, 'N') <> 'S';

  IF v_pendientes = 0 THEN
    UPDATE `fza_pedidos`
       SET `ESTADO_PED`    = 'ENTREGADO',
           `INSTANTEMODIF` = NOW(),
           `USUARIOMODIF`  = p_USUARIO
     WHERE `NUMERO_PED` = p_NUMERO_PED
       AND `SERIE_PED`  = p_SERIE_PED;
  ELSE
    UPDATE `fza_pedidos`
       SET `ESTADO_PED`    = 'PARCIAL',
           `INSTANTEMODIF` = NOW(),
           `USUARIOMODIF`  = p_USUARIO
     WHERE `NUMERO_PED` = p_NUMERO_PED
       AND `SERIE_PED`  = p_SERIE_PED;
  END IF;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_PED_CREAR_ALBARAN_INICIO
DROP PROCEDURE IF EXISTS `PRC_PED_CREAR_ALBARAN_INICIO`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_PED_CREAR_ALBARAN_INICIO`(
  IN  p_NUMERO_PED varchar(20),
  IN  p_SERIE_PED  varchar(20),
  IN  p_USUARIO    varchar(100),
  OUT p_NUMERO_ALB varchar(20),
  OUT p_SERIE_ALB  varchar(20)
)
BEGIN
  DECLARE v_serie  varchar(20);
  DECLARE v_numero varchar(20);

  /* Reusamos la serie del pedido para mantener consistencia */
  SELECT `SERIE_PED` INTO v_serie
    FROM `fza_pedidos`
   WHERE `NUMERO_PED` = p_NUMERO_PED
     AND `SERIE_PED`  = p_SERIE_PED;

  /* Próximo número en esa serie */
  SELECT LPAD(IFNULL(MAX(CAST(`NUMERO_ALB` AS UNSIGNED)), 0) + 1, 6, '0')
    INTO v_numero
    FROM `fza_albaranes`
   WHERE `SERIE_ALB` = v_serie;

  INSERT INTO `fza_albaranes` (
    `NUMERO_ALB`, `SERIE_ALB`, `FECHA_ALB`, `ESTADO_ALB`,
    `NUMERO_PED_ALB`, `SERIE_PED_ALB`,
    `CODIGO_EMP_ALB`, `RAZON_SOCIAL_EMPRESA_ALB`, `NIF_EMPRESA_ALB`,
    `MOVIL_EMPRESA_ALB`, `EMAIL_EMPRESA_ALB`,
    `DIRECCION1_EMPRESA_ALB`, `DIRECCION2_EMPRESA_ALB`,
    `POBLACION_EMPRESA_ALB`, `PROVINCIA_EMPRESA_ALB`,
    `CODIGO_PAI_EMPRESA_ALB`, `NOMBRE_PAI_EMPRESA_ALB`,
    `CODIGO_POSTAL_EMPRESA_ALB`, `GRUPO_ZONA_IVA_EMPRESA_ALB`,
    `CODIGO_CLI_ALB`, `RAZON_SOCIAL_CLIENTE_ALB`, `NIF_CLIENTE_ALB`,
    `MOVIL_CLIENTE_ALB`, `EMAIL_CLIENTE_ALB`,
    `DIRECCION1_CLIENTE_ALB`, `DIRECCION2_CLIENTE_ALB`,
    `POBLACION_CLIENTE_ALB`, `PROVINCIA_CLIENTE_ALB`,
    `CODIGO_POSTAL_CLIENTE_ALB`,
    `CODIGO_PAI_CLIENTE_ALB`, `NOMBRE_PAI_CLIENTE_ALB`,
    `NOMBRE_CLI_ENVIO_ALB`, `MOVIL_CLIENTE_ENVIO_ALB`,
    `DIRECCION1_CLIENTE_ENVIO_ALB`, `DIRECCION2_CLIENTE_ENVIO_ALB`,
    `POBLACION_CLIENTE_ENVIO_ALB`, `PROVINCIA_CLIENTE_ENVIO_ALB`,
    `CODIGO_POSTAL_CLIENTE_ENVIO_ALB`,
    `CODIGO_PAI_CLIENTE_ENVIO_ALB`, `NOMBRE_PAI_CLIENTE_ENVIO_ALB`,
    `TRANSPORTISTA_ALB`, `CODIGO_IVA_ALB`,
    `ESIVA_RECARGO_CLIENTE_ALB`, `ESIVA_EXENTO_CLIENTE_ALB`,
    `ESINTRACOMUNITARIO_CLIENTE_ALB`,
    `TARIFA_ARTICULO_CLIENTE_ALB`, `ESIMP_INCL_TARIFA_CLIENTE_ALB`,
    `PORCENTAJE_IVAN_ALB`, `PORCENTAJE_IVAR_ALB`,
    `PORCENTAJE_IVAS_ALB`, `PORCENTAJE_IVAE_ALB`,
    `FORMA_PAGO_ALB`, `CONTADOR_LINEAS_ALB`,
    `INSTANTE_ALTA`, `USUARIO_ALTA`, `USUARIO_MODIF`
  )
  SELECT
    v_numero, v_serie, CURRENT_DATE(), 'ABIERTO',
    p_NUMERO_PED, p_SERIE_PED,
    P.`CODIGO_EMP_PED`, P.`RAZON_SOCIAL_EMPRESA_PED`, P.`NIF_EMPRESA_PED`,
    P.`MOVIL_EMPRESA_PED`, P.`EMAIL_EMPRESA_PED`,
    P.`DIRECCION1_EMPRESA_PED`, P.`DIRECCION2_EMPRESA_PED`,
    P.`POBLACION_EMPRESA_PED`, P.`PROVINCIA_EMPRESA_PED`,
    P.`CODIGO_PAI_EMPRESA_PED`, P.`NOMBRE_PAI_EMPRESA_PED`,
    P.`CODIGO_POSTAL_EMPRESA_PED`, P.`GRUPO_ZONA_IVA_EMPRESA_PED`,
    P.`CODIGO_CLI_PED`, P.`RAZON_SOCIAL_CLIENTE_FISCAL_PED`,
    P.`NIF_CLIENTE_PED`, P.`MOVIL_CLIENTE_FISCAL_PED`,
    P.`EMAIL_CLIENTE_PED`,
    P.`DIRECCION1_CLIENTE_FISCAL_PED`, P.`DIRECCION2_CLIENTE_FISCAL_PED`,
    P.`POBLACION_CLIENTE_FISCAL_PED`, P.`PROVINCIA_CLIENTE_FISCAL_PED`,
    P.`CODIGO_POSTAL_CLIENTE_FISCAL_PED`,
    P.`CODIGO_PAI_CLIENTE_FISCAL_PED`, P.`NOMBRE_PAI_CLIENTE_FISCAL_PED`,
    P.`NOMBRE_CLI_ENVIO_PED`, P.`MOVIL_CLIENTE_ENVIO_PED`,
    P.`DIRECCION1_CLIENTE_ENVIO_PED`, P.`DIRECCION2_CLIENTE_ENVIO_PED`,
    P.`POBLACION_CLIENTE_ENVIO_PED`, P.`PROVINCIA_CLIENTE_ENVIO_PED`,
    P.`CODIGO_POSTAL_CLIENTE_ENVIO_PED`,
    P.`CODIGO_PAI_CLIENTE_ENVIO_PED`, P.`NOMBRE_PAI_CLIENTE_ENVIO_PED`,
    P.`TRANSPORTISTAPS_PED`, P.`CODIGO_IVA_PED`,
    P.`ESIVA_RECARGO_CLIENTE_PED`, P.`ESIVA_EXENTO_CLIENTE_PED`,
    P.`ESINTRACOMUNITARIO_CLIENTE_PED`,
    P.`TARIFA_ARTICULO_CLIENTE_PED`, P.`ESIMP_INCL_TARIFA_CLIENTE_PED`,
    P.`PORCENTAJE_IVAN_PED`, P.`PORCENTAJE_IVAR_PED`,
    P.`PORCENTAJE_IVAS_PED`, P.`PORCENTAJE_IVAE_PED`,
    P.`FORMA_PAGO_PED`, '0',
    NOW(), p_USUARIO, p_USUARIO
  FROM `fza_pedidos` P
  WHERE P.`NUMERO_PED` = p_NUMERO_PED
    AND P.`SERIE_PED`  = p_SERIE_PED;

  SET p_NUMERO_ALB = v_numero;
  SET p_SERIE_ALB  = v_serie;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_PED_CREAR_ALBARAN_LINEA
DROP PROCEDURE IF EXISTS `PRC_PED_CREAR_ALBARAN_LINEA`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_PED_CREAR_ALBARAN_LINEA`(
  IN  p_NUMERO_ALB    varchar(20),
  IN  p_SERIE_ALB     varchar(20),
  IN  p_NUMERO_PED    varchar(20),
  IN  p_SERIE_PED     varchar(20),
  IN  p_LINEA_PED     varchar(4),
  IN  p_CANTIDAD      decimal(19,6),
  IN  p_USUARIO       varchar(100)
)
PRC: BEGIN
  DECLARE v_linea     varchar(4);
  DECLARE v_pendiente decimal(19,6);
  DECLARE v_cantidad  decimal(19,6);

  SELECT (`CANTIDAD_PEDLIN` - IFNULL(`CANTIDAD_ENTREGADA_PEDLIN`, 0))
    INTO v_pendiente
    FROM `fza_pedidos_lineas`
   WHERE `NUMERO_PED_PEDLIN` = p_NUMERO_PED
     AND `SERIE_PED_PEDLIN`  = p_SERIE_PED
     AND `LINEA_PEDLIN`      = p_LINEA_PED;

  IF v_pendiente IS NULL OR v_pendiente <= 0 THEN
    LEAVE PRC;
  END IF;

  IF p_CANTIDAD > v_pendiente THEN
    SET v_cantidad = v_pendiente;
  ELSE
    SET v_cantidad = p_CANTIDAD;
  END IF;

  SELECT LPAD(IFNULL(MAX(CAST(`LINEA_ALBLIN` AS UNSIGNED)), 0) + 10, 4, '0')
    INTO v_linea
    FROM `fza_albaranes_lineas`
   WHERE `NUMERO_ALB_ALBLIN` = p_NUMERO_ALB
     AND `SERIE_ALB_ALBLIN`  = p_SERIE_ALB;

  INSERT INTO `fza_albaranes_lineas` (
    `NUMERO_ALB_ALBLIN`, `SERIE_ALB_ALBLIN`, `LINEA_ALBLIN`,
    `NUMERO_PED_ALBLIN`, `SERIE_PED_ALBLIN`, `LINEA_PED_ALBLIN`,
    `CODIGO_ART_ALBLIN`, `CODIGO_FAM_ALBLIN`, `NOMBRE_FAM_ALBLIN`,
    `DESCRIPCION_ARTICULO_ALBLIN`, `TIPO_CANTIDAD_ARTICULO_ALBLIN`,
    `CANTIDAD_ALBLIN`, `CODIGO_TAR_ALBLIN`, `ESIMP_INCL_TARIFA_ALBLIN`,
    `TIPO_IVA_ARTICULO_ALBLIN`, `PORCENTAJE_IVA_ALBLIN`,
    `PRECIO_VENTA_SIVA_ARTICULO_ALBLIN`,
    `PRECIO_VENTA_CIVA_ARTICULO_ALBLIN`,
    `TOTAL_ALBLIN`, `CODIGO_ALMACEN_ALBLIN`,
    `INSTANTE_ALTA`, `USUARIO_ALTA`, `USUARIO_MODIF`
  )
  SELECT
    p_NUMERO_ALB, p_SERIE_ALB, v_linea,
    p_NUMERO_PED, p_SERIE_PED, p_LINEA_PED,
    PL.`CODIGO_ART_PEDLIN`, PL.`CODIGO_FAM_PEDLIN`, PL.`NOMBRE_FAM_PEDLIN`,
    PL.`DESCRIPCION_ARTICULO_PEDLIN`, PL.`TIPO_CANTIDAD_ARTICULO_PEDLIN`,
    v_cantidad, PL.`CODIGO_TAR_PEDLIN`, PL.`ESIMP_INCL_TARIFA_PEDLIN`,
    PL.`TIPO_IVA_ARTICULO_PEDLIN`, PL.`PORCENTAJE_IVA_PEDLIN`,
    PL.`PRECIO_VENTA_SIVA_ARTICULO_PEDLIN`,
    PL.`PRECIO_VENTA_CIVA_ARTICULO_PEDLIN`,
    (v_cantidad * PL.`PRECIO_VENTA_SIVA_ARTICULO_PEDLIN`),
    PL.`CODIGO_ALMACEN_PEDLIN`,
    NOW(), p_USUARIO, p_USUARIO
  FROM `fza_pedidos_lineas` PL
  WHERE PL.`NUMERO_PED_PEDLIN` = p_NUMERO_PED
    AND PL.`SERIE_PED_PEDLIN`  = p_SERIE_PED
    AND PL.`LINEA_PEDLIN`      = p_LINEA_PED;

  UPDATE `fza_pedidos_lineas`
     SET `CANTIDAD_ENTREGADA_PEDLIN` = IFNULL(`CANTIDAD_ENTREGADA_PEDLIN`, 0) + v_cantidad,
         `CANTIDAD_PENDIENTE_PEDLIN` = `CANTIDAD_PEDLIN` - (IFNULL(`CANTIDAD_ENTREGADA_PEDLIN`, 0) + v_cantidad),
         `ESENTREGADA_PEDLIN`        = CASE WHEN `CANTIDAD_PEDLIN` <= IFNULL(`CANTIDAD_ENTREGADA_PEDLIN`, 0) + v_cantidad
                                            THEN 'S' ELSE 'N' END,
         `INSTANTEMODIF`             = NOW(),
         `USUARIOMODIF`              = p_USUARIO
   WHERE `NUMERO_PED_PEDLIN` = p_NUMERO_PED
     AND `SERIE_PED_PEDLIN`  = p_SERIE_PED
     AND `LINEA_PEDLIN`      = p_LINEA_PED;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_REALIZAR_TRASPASO
DROP PROCEDURE IF EXISTS `PRC_REALIZAR_TRASPASO`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_REALIZAR_TRASPASO`(
    IN pUsuario VARCHAR(50),
    IN pEmpresa VARCHAR(20),
    IN pAlmacenOrigen VARCHAR(10),
    IN pAlmacenDestino VARCHAR(10),
    IN pSku VARCHAR(50),
    IN pCantidad DECIMAL(19,6))
BEGIN
    DECLARE vSerie VARCHAR(20) DEFAULT 'TRAS';
    DECLARE vNroDoc VARCHAR(20);

    /* Manejo de errores para asegurar la consistencia */
    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
    kk: BEGIN
        ROLLBACK;
        RESIGNAL;
    END kk;

    /* Generamos un número de documento único basado en la fecha y hora */
    SET vNroDoc = DATE_FORMAT(NOW(), '%Y%m%d%H%i%s');

    START TRANSACTION;

    /* 1. SALIDA DEL ORIGEN (Resta stock en Origen) */
    INSERT INTO `fza_movimientos_almacen` 
    (TIPO_DOC_MOV, SERIE_DOC_MOV, NUMERO_DOC_MOV, LINEA_MOV, CODIGO_EMP_MOV,
      CODIGO_ALM_MOV, CODIGO_ALM_CONTRA_MOV, FECHA_MOV,
      CODIGO_UNIDAD_MOV, TIPO_MOV, CANTIDAD_MOV,
      DESCRIPCION_ARTICULO_MOV, USUARIO_ALTA, USUARIO_MODIF)
    VALUES 
    ('TR', vSerie, vNroDoc, '001', pEmpresa,
      pAlmacenOrigen, pAlmacenDestino, NOW(),
      pSku, 'S', pCantidad,
      CONCAT('Traspaso a ', pAlmacenDestino), pUsuario, pUsuario);

    /* 2. ENTRADA EN DESTINO (Suma stock en Destino) */
    INSERT INTO `fza_movimientos_almacen` 
    (TIPO_DOC_MOV, SERIE_DOC_MOV, NUMERO_DOC_MOV, LINEA_MOV, CODIGO_EMP_MOV,
      CODIGO_ALM_MOV, CODIGO_ALM_CONTRA_MOV, FECHA_MOV,
      CODIGO_UNIDAD_MOV, TIPO_MOV, CANTIDAD_MOV,
      DESCRIPCION_ARTICULO_MOV, USUARIO_ALTA, USUARIO_MODIF,
     TIPO_DOC_REF_MOV, SERIE_DOC_REF_MOV, NUMERO_DOC_REF_MOV, LINEA_REF_MOV)
    VALUES 
    ('TR', vSerie, vNroDoc, '002', pEmpresa,
      pAlmacenDestino, pAlmacenOrigen, NOW(),
      pSku, 'E', pCantidad,
      CONCAT('Traspaso desde ', pAlmacenOrigen), pUsuario, pUsuario,
     'TR', vSerie, vNroDoc, '001');

    COMMIT;

    SELECT CONCAT('Traspaso realizado. Doc: ', vSerie, '-', vNroDoc) AS MENSAJE;
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_RECALCULAR_STOCK
DROP PROCEDURE IF EXISTS `PRC_RECALCULAR_STOCK`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_RECALCULAR_STOCK`()
BEGIN
    /* Declaramos el manejador de errores */
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    bloque_error: BEGIN
        /* En caso de error, deshacer cambios */
        ROLLBACK;
        SELECT 'ERROR: No se pudo recalcular el stock' as MENSAJE;
    END bloque_error; /* ¡Le ponemos un nombre para que Delphi no se confunda! */

    START TRANSACTION;
    
    /* Borramos la tabla de saldos */
    DELETE FROM fza_articulos_stockactual;
    
    /* Volcamos los datos recalculados */
    INSERT INTO fza_articulos_stockactual 
        (CODIGO_ALM_STK, CODIGO_UNIDAD_STK, CANTIDAD_STK, INSTANTE_MODIF)
    SELECT 
        CODIGO_ALM_MOV,
        CODIGO_UNIDAD_MOV,
        SUM(IF(TIPO_MOV = 'E', CANTIDAD_MOV, -CANTIDAD_MOV)),
        NOW()
    FROM fza_movimientos_almacen
    GROUP BY CODIGO_ALM_MOV, CODIGO_UNIDAD_MOV;
        
    COMMIT;
    
    SELECT 'Stock recalculado correctamente.' as MENSAJE;
    
END ;;
DELIMITER ;

-- Recreando procedimiento: PRC_SETPERFILFORMULARIO
DROP PROCEDURE IF EXISTS `PRC_SETPERFILFORMULARIO`;

DELIMITER ;;
CREATE  PROCEDURE `PRC_SETPERFILFORMULARIO`(IN p_usuario_grupo  VARCHAR(200),
    IN p_formulario     VARCHAR(100),
    IN p_subkey         VARCHAR(100),
    IN p_value          VARCHAR(200))
BEGIN
    INSERT INTO fza_usuarios_perfiles 
        (USUARIO_GRUPO_USUPER, KEY_USUPER, SUBKEY_USUPER, 
         VALUE_USUPER, INSTANTE_MODIF, INSTANTE_ALTA, USUARIO_MODIF, USUARIO_ALTA)
    VALUES 
        (p_usuario_grupo, p_formulario, p_subkey, 
         p_value, NOW(), NOW(), p_usuario_grupo, p_usuario_grupo)
    ON DUPLICATE KEY UPDATE
        VALUE_USUPER  = p_value,
        INSTANTE_MODIF   = NOW(),
        USUARIO_MODIF    = p_usuario_grupo;
END ;;
DELIMITER ;

-- Recreando procedimiento: SP_RECALCULAR_PMP_LOTE_ALMACEN
DROP PROCEDURE IF EXISTS `SP_RECALCULAR_PMP_LOTE_ALMACEN`;

DELIMITER ;;
CREATE  PROCEDURE `SP_RECALCULAR_PMP_LOTE_ALMACEN`(
    IN p_EMPRESA VARCHAR(20),
    IN p_ALMACEN VARCHAR(10)
)
BEGIN
    /* 1. Tabla con todos los movs de los SKUs afectados, ordenados.
       El INSERT...SELECT ORDER BY garantiza el orden de RN secuencial
       (RN es AUTO_INCREMENT como PK clustered en InnoDB). */
    DROP TEMPORARY TABLE IF EXISTS tmp_movs_ord;
    CREATE TEMPORARY TABLE tmp_movs_ord (
        RN                        BIGINT          NOT NULL AUTO_INCREMENT,
        NUMERO_MOV                VARCHAR(20)     NOT NULL,
        CODIGO_UNIDAD_MOV         VARCHAR(50)     NOT NULL,
        TIPO_MOV                  VARCHAR(1)      NOT NULL,
        CANTIDAD_MOV              DECIMAL(19,6)   NOT NULL,
        PRECIO_COSTE_UNITARIO_MOV DECIMAL(19,6)   NOT NULL,
        PMP_NUEVO                 DECIMAL(19,6)   NOT NULL DEFAULT 0,
        STOCK_NUEVO               DECIMAL(19,6)   NOT NULL DEFAULT 0,
        COSTE_NUEVO               DECIMAL(19,6)   NOT NULL DEFAULT 0,
        SKU_PREV                  VARCHAR(50)     NULL,
        PRIMARY KEY (RN),
        KEY IDX_NUMMOV (NUMERO_MOV),
        KEY IDX_SKU    (CODIGO_UNIDAD_MOV)
    ) ENGINE=InnoDB;

    INSERT INTO tmp_movs_ord
        (NUMERO_MOV, CODIGO_UNIDAD_MOV, TIPO_MOV, CANTIDAD_MOV,
         PRECIO_COSTE_UNITARIO_MOV)
    SELECT m.NUMERO_MOV,
           m.CODIGO_UNIDAD_MOV,
           m.TIPO_MOV,
           IFNULL(m.CANTIDAD_MOV, 0),
           IFNULL(m.PRECIO_COSTE_UNITARIO_MOV, 0)
      FROM fza_movimientos_almacen m
      JOIN tmp_skus_recalc s ON s.sku = m.CODIGO_UNIDAD_MOV
     WHERE m.CODIGO_ALM_MOV = p_ALMACEN
     ORDER BY m.CODIGO_UNIDAD_MOV, m.FECHA_MOV, m.INSTANTE_ALTA;

    /* 2. Calculo acumulado por SKU con variables de sesion. El ORDER BY RN
       fuerza el barrido secuencial por la PK clustered. Las asignaciones del
       SET se evaluan de izquierda a derecha: primero PMP_NUEVO (usa @stock y
       @pmp todavia con valor de la fila anterior + @sku_prev de la fila
       anterior para detectar cambio), luego COSTE_NUEVO (usa el @pmp recien
       calculado), luego STOCK_NUEVO (actualiza @stock), y al final SKU_PREV
       actualiza @sku_prev para la siguiente fila. */
    SET @sku_prev := '';
    SET @stock    := 0;
    SET @pmp      := 0;

    UPDATE tmp_movs_ord
       SET PMP_NUEVO = (
               @pmp := IF(
                   @sku_prev <> CODIGO_UNIDAD_MOV,
                   /* Cambio de SKU: empezamos desde cero */
                   IF(TIPO_MOV = 'E', PRECIO_COSTE_UNITARIO_MOV, 0),
                   /* Mismo SKU que la fila anterior */
                   IF(TIPO_MOV = 'E',
                       IF(@stock <= 0,
                           PRECIO_COSTE_UNITARIO_MOV,
                           ((@stock * @pmp)
                              + (CANTIDAD_MOV * PRECIO_COSTE_UNITARIO_MOV))
                            / (@stock + CANTIDAD_MOV)),
                       @pmp
                   )
               )
           ),
           COSTE_NUEVO = IF(TIPO_MOV = 'E',
                            CANTIDAD_MOV * PRECIO_COSTE_UNITARIO_MOV,
                            CANTIDAD_MOV * @pmp),
           STOCK_NUEVO = (
               @stock := IF(
                   @sku_prev <> CODIGO_UNIDAD_MOV,
                   /* Cambio de SKU: stock empieza en 0 y se suma este mov */
                   IF(TIPO_MOV = 'E', CANTIDAD_MOV, -CANTIDAD_MOV),
                   IF(TIPO_MOV = 'E', @stock + CANTIDAD_MOV,
                                       @stock - CANTIDAD_MOV)
               )
           ),
           SKU_PREV = (@sku_prev := CODIGO_UNIDAD_MOV)
     ORDER BY RN;

    /* 3. Volcado en lote a fza_movimientos_almacen. Un solo UPDATE...JOIN
       toma X-locks sobre los movs afectados y los libera al terminar. */
    UPDATE fza_movimientos_almacen m
      JOIN tmp_movs_ord t ON t.NUMERO_MOV = m.NUMERO_MOV
       SET m.PRECIO_MEDIO_MOV = t.PMP_NUEVO,
           m.TOTAL_COSTE_MOV  = t.COSTE_NUEVO;

    /* 4. Stock final por SKU: el ultimo mov (RN maximo) tiene el stock y
       el PMP acumulados al final del historico. Volcamos a stockactual. */
    INSERT INTO fza_articulos_stockactual
        (CODIGO_ALM_STK, CODIGO_UNIDAD_STK,
         CANTIDAD_STK, VALOR_TOTAL_STK, PRECIO_MEDIO_STK, INSTANTE_MODIF)
    SELECT p_ALMACEN,
           t.CODIGO_UNIDAD_MOV,
           t.STOCK_NUEVO,
           IF(t.STOCK_NUEVO > 0, t.STOCK_NUEVO * t.PMP_NUEVO, 0),
           IF(t.STOCK_NUEVO > 0, t.PMP_NUEVO, 0),
           NOW()
      FROM tmp_movs_ord t
      JOIN (
            SELECT CODIGO_UNIDAD_MOV, MAX(RN) AS RN_MAX
              FROM tmp_movs_ord
             GROUP BY CODIGO_UNIDAD_MOV
           ) ult
        ON ult.CODIGO_UNIDAD_MOV = t.CODIGO_UNIDAD_MOV
       AND ult.RN_MAX             = t.RN
     ON DUPLICATE KEY UPDATE
        CANTIDAD_STK     = VALUES(CANTIDAD_STK),
        VALOR_TOTAL_STK  = VALUES(VALOR_TOTAL_STK),
        PRECIO_MEDIO_STK = VALUES(PRECIO_MEDIO_STK),
        INSTANTE_MODIF   = NOW();

    /* 5. SKUs sin movs sobrevivientes (todos sus movs estaban en la
       regularizacion borrada): el stockactual queda a 0. */
    INSERT INTO fza_articulos_stockactual
        (CODIGO_ALM_STK, CODIGO_UNIDAD_STK,
         CANTIDAD_STK, VALOR_TOTAL_STK, PRECIO_MEDIO_STK, INSTANTE_MODIF)
    SELECT p_ALMACEN, s.sku, 0, 0, 0, NOW()
      FROM tmp_skus_recalc s
      LEFT JOIN tmp_movs_ord t ON t.CODIGO_UNIDAD_MOV = s.sku
     WHERE t.CODIGO_UNIDAD_MOV IS NULL
     ON DUPLICATE KEY UPDATE
        CANTIDAD_STK     = 0,
        VALOR_TOTAL_STK  = 0,
        PRECIO_MEDIO_STK = 0,
        INSTANTE_MODIF   = NOW();

    DROP TEMPORARY TABLE IF EXISTS tmp_movs_ord;
END ;;
DELIMITER ;

-- Recreando procedimiento: SP_RECALCULAR_PMP_SKU
DROP PROCEDURE IF EXISTS `SP_RECALCULAR_PMP_SKU`;

DELIMITER ;;
CREATE  PROCEDURE `SP_RECALCULAR_PMP_SKU`(IN `p_CodigoEmpresa` VARCHAR(20),
    IN `p_CodigoSKU` VARCHAR(50),
    IN `p_FechaDesde` DATETIME)
BEGIN
    /* Variables para el bucle */
    DECLARE done INT DEFAULT FALSE;
    DECLARE vIdTipo VARCHAR(5);
    DECLARE vIdSerie VARCHAR(20);
    DECLARE vIdNro VARCHAR(20);
    DECLARE vIdLinea VARCHAR(4);
    
    DECLARE vTipoMov VARCHAR(1);
    DECLARE vCantidad DECIMAL(19,6);
    DECLARE vPrecioCoste DECIMAL(19,6);
    
    /* Variables para el cálculo acumulado */
    DECLARE vStockAcumulado DECIMAL(19,6) DEFAULT 0;
    DECLARE vPMP_Actual DECIMAL(19,6) DEFAULT 0;
    
    /* CURSOR: Seleccionamos TODOS los movimientos de ese SKU ordenados cronológicamente */
    /* Es vital incluir movimientos ANTERIORES a la fecha para coger el saldo inicial correcto */
    DECLARE curMovimientos CURSOR FOR 
        SELECT 
            TIPO_DOC_MOV, SERIE_DOC_MOV, NUMERO_DOC_MOV, LINEA_MOV,
            TIPO_MOV, CANTIDAD_MOV, PRECIO_COSTE_UNITARIO_MOV, FECHA_MOV
        FROM fza_movimientos_almacen
        WHERE CODIGO_EMP_MOV = p_CodigoEmpresa
          AND CODIGO_UNIDAD_MOV = p_CodigoSKU
        ORDER BY FECHA_MOV ASC, INSTANTE_ALTA ASC;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    /* 1. Inicialización: Limpiamos variables */
    SET vStockAcumulado = 0;
    SET vPMP_Actual = 0;

    OPEN curMovimientos;

    read_loop: LOOP
        FETCH curMovimientos INTO vIdTipo, vIdSerie, vIdNro, vIdLinea, vTipoMov, vCantidad, vPrecioCoste, p_FechaDesde; /* Reusamos p_FechaDesde solo para leer fecha, no afecta lógica */
        IF done THEN
            LEAVE read_loop;
        END IF;

        /* --------------------------------------------------------- */
        /* LÓGICA DE CÁLCULO (Idéntica a la del Trigger, pero en bucle) */
        /* --------------------------------------------------------- */
        
        IF vTipoMov = 'E' THEN
            /* Es ENTRADA */
            /* Si el stock venía de negativo o cero, reiniciamos precio */
            IF vStockAcumulado <= 0 THEN
                SET vPMP_Actual = vPrecioCoste;
            ELSE
                /* Fórmula PMP */
                SET vPMP_Actual = (
                    (vStockAcumulado * vPMP_Actual) + (vCantidad * vPrecioCoste)
                ) / (vStockAcumulado + vCantidad);
            END IF;
            
            /* Actualizamos el Stock Acumulado sumando */
            SET vStockAcumulado = vStockAcumulado + vCantidad;
            
        ELSE
            /* Es SALIDA */
            /* El PMP no cambia, se mantiene el que traíamos */
            /* Pero el stock baja */
            SET vStockAcumulado = vStockAcumulado - vCantidad;
        END IF;

        /* --------------------------------------------------------- */
        /* ACTUALIZACIÓN DE LA FILA */
        /* --------------------------------------------------------- */
        /* Actualizamos el campo PRECIO_MEDIO_MOV de esta fila específica */
        /* Nota: Esto disparará el Trigger UPDATE. Para evitar bucles, */
        /* el trigger UPDATE debería detectar si el valor ya es igual para no hacer nada, */
        /* o deshabilitar triggers temporalmente si fuera necesario. */
        
        UPDATE fza_movimientos_almacen 
        SET PRECIO_MEDIO_MOV = vPMP_Actual
        WHERE TIPO_DOC_MOV = vIdTipo 
          AND SERIE_DOC_MOV = vIdSerie
          AND NUMERO_DOC_MOV = vIdNro
          AND LINEA_MOV = vIdLinea;
          
    END LOOP;

    CLOSE curMovimientos;
END ;;
DELIMITER ;

-- Recreando procedimiento: SP_RECALCULAR_PMP_SKU_ALMACEN
DROP PROCEDURE IF EXISTS `SP_RECALCULAR_PMP_SKU_ALMACEN`;

DELIMITER ;;
CREATE  PROCEDURE `SP_RECALCULAR_PMP_SKU_ALMACEN`(
    IN p_CodigoEmpresa VARCHAR(20),
    IN p_CodigoSKU     VARCHAR(50),
    IN p_CodigoAlmacen VARCHAR(10)
)
BEGIN
    DROP TEMPORARY TABLE IF EXISTS tmp_skus_recalc;
    CREATE TEMPORARY TABLE tmp_skus_recalc (
        sku VARCHAR(50) NOT NULL PRIMARY KEY
    ) ENGINE=InnoDB;

    INSERT INTO tmp_skus_recalc (sku) VALUES (p_CodigoSKU);

    CALL SP_RECALCULAR_PMP_LOTE_ALMACEN(p_CodigoEmpresa, p_CodigoAlmacen);

    DROP TEMPORARY TABLE IF EXISTS tmp_skus_recalc;
END ;;
DELIMITER ;

-- === FUNCIONES ===
