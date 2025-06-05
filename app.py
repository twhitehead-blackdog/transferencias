def validate_and_process_file(uploaded_file, db, uid, password, models):
    try:
        content = uploaded_file.read().decode('latin-1')
        df = pd.read_csv(io.StringIO(content), sep=";", dtype=str)
        df.columns = df.columns.str.replace('\ufeff', '', regex=False).str.strip().str.upper()
        columnas = list(df.columns)

        validation_results = {
            'is_valid': True,
            'format_detected': None,
            'errors': [],
            'warnings': [],
            'data_by_location': {},
            'column_mapping': {},
            'total_items': len(df)
        }

        columnas_formato1 = {
            'COD_BARRA': ['COD_BARRA', 'CODBARRA', 'CODIGO_BARRA', 'CODIGOBARRAS', 'BARCODE'],
            'CANTIDAD': ['CANTIDAD', 'CANT', 'QTY', 'QUANTITY'],
            'TIENDA_DESTINO': ['NBR_CLIENTE', 'TIENDA', 'DESTINO', 'SUCURSAL']
        }

        columnas_formato2 = {
            'CÓDIGO': ['CÓDIGO', 'CODIGO', 'CODE', 'COD', 'BARCODE'],
            'REFERENCIA INTERNA': ['REFERENCIA INTERNA', 'REFERENCIAINTERNA', 'REF_INTERNA', 'INTERNAL_REFERENCE'],
            'SUCURSAL': ['SUCURSAL', 'BODEGA', 'ALMACEN', 'TIENDA', 'WAREHOUSE'],
            'SURTIDO': ['SURTIDO', 'CANTIDAD', 'CANT', 'QTY', 'QUANTITY']
        }

        def encontrar_columna(posibles_nombres, columnas_df):
            for col in posibles_nombres:
                if col in columnas_df:
                    return col
            return None

        formato1_cols = {k: encontrar_columna(v, columnas) for k, v in columnas_formato1.items()}
        formato2_cols = {k: encontrar_columna(v, columnas) for k, v in columnas_formato2.items()}

        if all(formato1_cols.values()):
            validation_results['format_detected'] = 'FORMATO1'
            validation_results['column_mapping'] = formato1_cols
            df = df.rename(columns={v: k for k, v in formato1_cols.items()})
            grupo_por = 'TIENDA_DESTINO'
        elif all(formato2_cols.values()):
            validation_results['format_detected'] = 'FORMATO2'
            validation_results['column_mapping'] = formato2_cols
            df = df.rename(columns={v: k for k, v in formato2_cols.items()})
            grupo_por = 'SUCURSAL'
        else:
            validation_results['is_valid'] = False
            validation_results['errors'].append({
                'type': 'FORMAT_ERROR',
                'message': 'Formato de archivo no reconocido',
                'details': f'Columnas encontradas: {columnas}'
            })
            return validation_results

        for location, items in df.groupby(grupo_por):
            location = location.strip().upper()
            destino = alias_map.get(location, location)

            location_data = {
                'valid_items': [],
                'invalid_items': [],
                'total_items': len(items),
                'location_valid': True,
                'original_name': location
            }

            if destino not in location_map or destino not in picking_type_map:
                location_data['location_valid'] = False
                location_data['error'] = f"Ubicación no válida: {location}"
                validation_results['is_valid'] = False
                validation_results['data_by_location'][destino] = location_data
                continue

            for idx, row in items.iterrows():
                item_validation = {
                    'row_index': idx + 2,
                    'is_valid': True,
                    'errors': []
                }

                if validation_results['format_detected'] == 'FORMATO1':
                    codigo = str(row['COD_BARRA']).strip().replace(" ", "").replace("-", "")
                    try:
                        cantidad = float(row['CANTIDAD'])
                        if cantidad <= 0:
                            raise ValueError("La cantidad debe ser mayor que 0")
                    except ValueError as e:
                        item_validation['is_valid'] = False
                        item_validation['errors'].append(f"Cantidad inválida: {row['CANTIDAD']} - {str(e)}")
                else:
                    codigo = str(row['CÓDIGO']).strip().replace(" ", "").replace("-", "")
                    referencia = str(row['REFERENCIA INTERNA']).strip()
                    try:
                        cantidad = float(row['SURTIDO'])
                        if cantidad <= 0:
                            raise ValueError("La cantidad debe ser mayor que 0")
                    except ValueError as e:
                        item_validation['is_valid'] = False
                        item_validation['errors'].append(f"Cantidad inválida: {row['SURTIDO']} - {str(e)}")

                productos = models.execute_kw(db, uid, password,
                    'product.product', 'search_read',
                    [[['barcode', '=', codigo]]],
                    {'fields': ['id', 'name', 'uom_id'], 'limit': 1})

                if not productos and validation_results['format_detected'] == 'FORMATO2':
                    productos = models.execute_kw(db, uid, password,
                        'product.product', 'search_read',
                        [[['default_code', '=', referencia]]],
                        {'fields': ['id', 'name', 'uom_id'], 'limit': 1})

                if not productos:
                    item_validation['is_valid'] = False
                    error_msg = f"Producto no encontrado - Código de barras: {codigo}"
                    if validation_results['format_detected'] == 'FORMATO2':
                        error_msg += f", Referencia: {referencia}"
                    item_validation['errors'].append(error_msg)

                if item_validation['is_valid']:
                    item_validation['product_data'] = productos[0]
                    item_validation['quantity'] = cantidad
                    location_data['valid_items'].append(item_validation)
                else:
                    location_data['invalid_items'].append(item_validation)
                    validation_results['is_valid'] = False

            validation_results['data_by_location'][destino] = location_data

        return validation_results

    except Exception as e:
        validation_results['is_valid'] = False
        validation_results['errors'].append({
            'type': 'SYSTEM_ERROR',
            'message': str(e),
            'traceback': traceback.format_exc()
        })
        return validation_results

def create_transfers(validation_results, db, uid, password, models):
    transfer_results = {
        'success': True,
        'transfers_created': [],
        'errors': []
    }

    try:
        for destino, location_data in validation_results['data_by_location'].items():
            if not location_data['valid_items']:
                continue

            picking_id = models.execute_kw(db, uid, password, 'stock.picking', 'create', [{
                'picking_type_id': picking_type_map[destino],
                'location_id': location_map["BODEGA"],
                'location_dest_id': location_map[destino],
                'origin': f"Auto-importación {location_data['original_name']} - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            }])

            transfer_info = {
                'picking_id': picking_id,
                'location': destino,
                'original_name': location_data['original_name'],
                'items_processed': 0,
                'items_failed': 0
            }

            for item in location_data['valid_items']:
                try:
                    models.execute_kw(db, uid, password, 'stock.move', 'create', [{
                        'name': item['product_data']['name'],
                        'product_id': item['product_data']['id'],
                        'product_uom_qty': item['quantity'],
                        'product_uom': item['product_data']['uom_id'][0],
                        'picking_id': picking_id,
                        'location_id': location_map["BODEGA"],
                        'location_dest_id': location_map[destino],
                    }])
                    transfer_info['items_processed'] += 1
                except Exception as e:
                    transfer_info['items_failed'] += 1
                    transfer_results['errors'].append({
                        'picking_id': picking_id,
                        'product_id': item['product_data']['id'],
                        'error': str(e)
                    })

            transfer_results['transfers_created'].append(transfer_info)

    except Exception as e:
        transfer_results['success'] = False
        transfer_results['errors'].append({
            'type': 'SYSTEM_ERROR',
            'message': str(e),
            'traceback': traceback.format_exc()
        })

    return transfer_results

def main():
    show_centered_logo("logo.png", width=220)
    st.markdown("<div class='main-title'>Transferencias Black Dog</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='subtitle'>Sistema de transferencias automáticas entre sucursales.<br>Rápido, seguro y eficiente.</div>",
        unsafe_allow_html=True
    )
    st.markdown("""
        <div class='welcome-card'>
            <b>¿Cómo funciona?</b><br>
            <ul style='color:#FAB803;'>
                <li>Sube uno o varios archivos TXT con el formato establecido.</li>
                <li>El sistema validará automáticamente los datos.</li>
                <li>Se crearán las transferencias en Odoo si todo está correcto.</li>
            </ul>
        </div>
    """, unsafe_allow_html=True)

    try:
        with st.spinner("Conectando a Odoo..."):
            db, uid, password, models = get_odoo_connection()
        st.markdown("<div style='text-align:center; color:#00ff00;'>✅ Conectado a Odoo</div>", unsafe_allow_html=True)
    except Exception as e:
        st.markdown("<div style='text-align:center; color:#ff0000;'>❌ Error de conexión a Odoo</div>", unsafe_allow_html=True)
        st.error(f"Error: {str(e)}")
        return

    st.markdown("<div class='upload-section'>", unsafe_allow_html=True)
    uploaded_files = st.file_uploader(
        "Arrastra o selecciona los archivos TXT a procesar",
        accept_multiple_files=True,
        help="Solo se aceptan archivos TXT con el formato correcto"
    )
    st.markdown("</div>", unsafe_allow_html=True)

    if uploaded_files:
        valid_files = []
        ignored_files = []

        for f in uploaded_files:
            if f.name.lower().endswith('.txt'):
                valid_files.append(f)
            else:
                ignored_files.append(f.name)

        if ignored_files:
            st.warning("⚠️ Algunos archivos fueron ignorados por no tener formato TXT")
            st.write("❌ Archivos ignorados:", ignored_files)

        if valid_files:
            st.info(f"📁 {len(valid_files)} archivo(s) listo(s) para procesar")
            with st.expander("Ver archivos a procesar"):
                for f in valid_files:
                    st.write(f"📄 {f.name}")

            if st.button("Procesar Archivos", type="primary"):
                for uploaded_file in valid_files:
                    st.markdown(f"<div class='results-section'>", unsafe_allow_html=True)
                    st.write(f"### 📄 Procesando: {uploaded_file.name}")

                    with st.spinner("Validando datos..."):
                        validation_results = validate_and_process_file(uploaded_file, db, uid, password, models)

                    if validation_results.get('format_detected'):
                        st.success(f"✅ Formato detectado: {validation_results['format_detected']}")
                        st.info(f"📊 Total de registros: {validation_results.get('total_items', 0)}")

                    if validation_results.get('errors'):
                        for error in validation_results['errors']:
                            st.error(error['message'])

                    for destino, location_data in validation_results.get('data_by_location', {}).items():
                        st.write(f"#### 📍 {location_data['original_name']}")
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.write(f"📦 Total: {location_data['total_items']}")
                        with col2:
                            st.write(f"✅ Válidos: {len(location_data['valid_items'])}")
                        with col3:
                            st.write(f"❌ Errores: {len(location_data['invalid_items'])}")

                        if location_data['invalid_items']:
                            with st.expander("Ver errores"):
                                for item in location_data['invalid_items']:
                                    st.write(f"- Línea {item['row_index']}: {', '.join(item['errors'])}")

                    if validation_results['is_valid']:
                        with st.spinner("Creando transferencias..."):
                            transfer_results = create_transfers(validation_results, db, uid, password, models)
                            if transfer_results['success']:
                                for transfer in transfer_results['transfers_created']:
                                    st.success(f"✅ Transferencia {transfer['picking_id']} creada")
                                    st.write(f"Productos procesados: {transfer['items_processed']}")
                            else:
                                st.error("❌ Error al crear transferencias")
                                for error in transfer_results['errors']:
                                    st.write(error['message'])
                    else:
                        st.error("❌ No se pueden crear transferencias - Corrige los errores")

                    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown(
        "<hr><div style='text-align:center; color:#FAB803; font-size:0.95em;'>"
        "Desarrollado para Black Dog Panamá &copy; 2025</div>",
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
