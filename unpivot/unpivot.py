import pandas as pd

#print("Programma di Unpivoting - prendere dati da covid-19-marche.csv")

filename = 'covid-19-marche.csv'
df= pd.read_csv(filename)   #carico il dataframe e poi faccio le operazioni sul dataframe già fatto

df_data = df.loc[:, ['data']]

# ETL per malati totali divisi per province
df_pulito = df.loc[:, ['malati_totali_provincia_pesaro_urbino',
						'malati_totali_provincia_ancona',
						'malati_totali_provincia_macerata',
						'malati_totali_provincia_fermo',
						'malati_totali_provincia_ascoli_piceno',
						'malati_totali_provincia_extra_regione']]
df_pulito = df_pulito.rename(columns={"malati_totali_provincia_pesaro_urbino": "PU",
										"malati_totali_provincia_ancona": "AN",
										"malati_totali_provincia_macerata": "MC",
										"malati_totali_provincia_fermo": "FM",
										"malati_totali_provincia_ascoli_piceno": "AP",
										"malati_totali_provincia_extra_regione":"ND"})

df_pulito = df_pulito.unstack().reset_index(name='malati_totali')

new = []
for ind in df_pulito.index:
    i = df_pulito['level_1'][ind]
    data = df_data['data'][i]
    prov = df_pulito['level_0'][ind]
    num_malati = df_pulito['malati_totali'][ind]
    #print(data, df['Provincia'][ind], i)
    new.append({'data':data, 'provincia':prov, 'malati_totali':num_malati})

df_malati = pd.DataFrame(new)
#df_malati = df_malati.sort_values(by=['Data'])

# ETL per malati attivi - situazione dei ricoverati odierni
df_pulito = df.loc[:, [#'malati_attivi_odierni',
						#'malati_attivi_isolamento_domiciliare',
						#'malati_attivi_ricoverati_totali',
						#'malati_quarantena_domiciliare_attivi_totali',
						'malati_attivi_ricoverati_terapia_intensiva',
						'malati_attivi_ricoverati_semi_intensiva_totali',
						'malati_attivi_ricoverati_post_critica_totali',
						'malati_attivi_ricoverati_non_intensiva_totali']]
df_pulito = df_pulito.rename(columns={#'malati_attivi_odierni':'tot',
						#'malati_attivi_isolamento_domiciliare':'isolamento domiciliare',
						#'malati_attivi_ricoverati_totali':'ricoverati',
						#'malati_quarantena_domiciliare_attivi_totali':'quarantena'
						'malati_attivi_ricoverati_terapia_intensiva':'intensiva',
						'malati_attivi_ricoverati_semi_intensiva_totali':'semi-intensiva',
						'malati_attivi_ricoverati_post_critica_totali':'post-critica',
						'malati_attivi_ricoverati_non_intensiva_totali':'non intensiva'})
df_pulito = df_pulito.unstack().reset_index(name='Totali')

new = []
for ind in df_pulito.index:
    i = df_pulito['level_1'][ind]
    data = df_data['data'][i]
    ricovero = df_pulito['level_0'][ind]
    tot = df_pulito['Totali'][ind]
    #print(data, df['Provincia'][ind], i)
    new.append({'data':data, 'tipo_di_ricovero':ricovero, 'totali_tipi_ricovero':tot})

df_ricoveri = pd.DataFrame(new)
#df_ricoveri = df_ricoveri.sort_values(by=['Data'])

# ETL per malati attivi - situazione delle quarantene odierne - si potrebbe unire con quello sopra
df_pulito = df.loc[:, [	'malati_quarantena_domiciliare_attivi_sintomatici',
						'malati_quarantena_domiciliare_attivi_asintomatici']]
df_pulito = df_pulito.rename(columns={'malati_quarantena_domiciliare_attivi_sintomatici':'sintomatici',
						'malati_quarantena_domiciliare_attivi_asintomatici':'asintomatici'})
df_pulito = df_pulito.unstack().reset_index(name='Totali')

new = []
for ind in df_pulito.index:
    i = df_pulito['level_1'][ind]
    data = df_data['data'][i]
    quarantena = df_pulito['level_0'][ind]
    tot = df_pulito['Totali'][ind]
    #print(data, df['Provincia'][ind], i)
    new.append({'data':data, 'tipo_di_quarantena':quarantena, 'totali_tipi_quarantena':tot})

df_quarantena = pd.DataFrame(new)
#df_quarantena = df_quarantena.sort_values(by=['Data'])

# ETL per quarantena per provincia - odierni totali, sintomatici, asintomatici (si potrebbe unire con il primo dei malati per province)
df_pulito_od = df.loc[:, ['malati_quarantena_domiciliare_provincia_pesaro_urbino_attivi_odierni',
						'malati_quarantena_domiciliare_provincia_ancona_attivi_odierni',
						'malati_quarantena_domiciliare_provincia_macerata_attivi_odierni',
						'malati_quarantena_domiciliare_provincia_fermo_attivi_odierni',
						'malati_quarantena_domiciliare_provincia_ascoli_piceno_attivi_odierni']]
df_pulito_od = df_pulito_od.rename(columns={'malati_quarantena_domiciliare_provincia_pesaro_urbino_attivi_odierni':'PU',
						'malati_quarantena_domiciliare_provincia_ancona_attivi_odierni':'AN',
						'malati_quarantena_domiciliare_provincia_macerata_attivi_odierni':'MC',
						'malati_quarantena_domiciliare_provincia_fermo_attivi_odierni':'FM',
						'malati_quarantena_domiciliare_provincia_ascoli_piceno_attivi_odierni':'AP'})
df_pulito_od = df_pulito_od.unstack().reset_index(name='Totali')

df_pulito_as = df.loc[:, ['malati_quarantena_domiciliare_provincia_pesaro_urbino_attivi_sintomatici',
						'malati_quarantena_domiciliare_provincia_ancona_attivi_sintomatici',
						'malati_quarantena_domiciliare_provincia_macerata_attivi_sintomatici',
						'malati_quarantena_domiciliare_provincia_fermo_attivi_sintomatici',
						'malati_quarantena_domiciliare_provincia_ascoli_piceno_attivi_sintomatici']]
df_pulito_as = df_pulito_as.rename(columns={'malati_quarantena_domiciliare_provincia_pesaro_urbino_attivi_sintomatici':'PU',
						'malati_quarantena_domiciliare_provincia_ancona_attivi_sintomatici':'AN',
						'malati_quarantena_domiciliare_provincia_macerata_attivi_sintomatici':'MC',
						'malati_quarantena_domiciliare_provincia_fermo_attivi_sintomatici':'FM',
						'malati_quarantena_domiciliare_provincia_ascoli_piceno_attivi_sintomatici':'AP'})
df_pulito_as = df_pulito_as.unstack().reset_index(name='Totali')

df_pulito_aa = df.loc[:, ['malati_quarantena_domiciliare_provincia_pesaro_urbino_attivi_asintomatici',
						'malati_quarantena_domiciliare_provincia_ancona_attivi_asintomatici',
						'malati_quarantena_domiciliare_provincia_macerata_attivi_asintomatici',
						'malati_quarantena_domiciliare_provincia_fermo_attivi_asintomatici',
						'malati_quarantena_domiciliare_provincia_ascoli_piceno_attivi_asintomatici']]
df_pulito_aa = df_pulito_aa.rename(columns={'malati_quarantena_domiciliare_provincia_pesaro_urbino_attivi_asintomatici':'PU',
						'malati_quarantena_domiciliare_provincia_ancona_attivi_asintomatici':'AN',
						'malati_quarantena_domiciliare_provincia_macerata_attivi_asintomatici':'MC',
						'malati_quarantena_domiciliare_provincia_fermo_attivi_asintomatici':'FM',
						'malati_quarantena_domiciliare_provincia_ascoli_piceno_attivi_asintomatici':'AP'})
df_pulito_aa = df_pulito_aa.unstack().reset_index(name='Totali')

new = []
for ind in df_pulito_od.index:
    i = df_pulito_od['level_1'][ind]
    data = df_data['data'][i]
    prov = df_pulito_od['level_0'][ind]

    prov_as = df_pulito_as['level_0'][ind]
    prov_aa = df_pulito_aa['level_0'][ind]

    tot = df_pulito_od['Totali'][ind]
    tot_as = df_pulito_as['Totali'][ind]
    tot_aa = df_pulito_aa['Totali'][ind]
    #print(data, df['Provincia'][ind], i)
    new.append({'data':data, 'provincia':prov, 'totali_in_quarantena':tot, 'sintomatici':tot_as, 'asintomatici':tot_aa})

df_quarantena_prov = pd.DataFrame(new)
#df_quarantena = df_quarantena.sort_values(by=['Data'])

df_malati.to_csv('malati_per_province_totali.csv')
df_ricoveri.to_csv('ricoveri_per_tipi.csv')
df_quarantena.to_csv('quarantena_per_tipi_attivi.csv')
df_quarantena_prov.to_csv('quarantena_per_province.csv')