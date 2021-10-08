from criteriaetl.transformers.columns_base import (NameTransformer, 
        ReplaceTransformer, SelectTransformer, AssignTransformer)
from criteriaetl.transformers.columns_base import BernoulliShockTransformer

class BernoulliShockWrapperTransformer(BernoulliShockTransformer):
        def transform(self, df, verbose=False):
                shuffled_df = df.sample(frac=1, random_state=6202)
                for i, args in enumerate(self.arg_list):
                        arg2 = args[2](shuffled_df) if callable(args[2]) else args[2]
                        print(arg2) if verbose else None
                        partial_col = f'{args[1]}_{i}'
                        if args[1] in shuffled_df.columns:
                                print(
                                        '*'*10, 
                                        shuffled_df.loc[args[3](shuffled_df), args[1]].value_counts(),
                                        '*'*10) if verbose else None
                        shuffled_df = self._BernoulliShockTransformer__complete_randomization_shock(shuffled_df,
                                                                                                        args[0], partial_col,
                                                                                                                        1-arg2, args[3])
                        try:
                                shuffled_df[args[1]] = shuffled_df[args[1]] +\
                                                                                        shuffled_df[partial_col]
                        except:
                                shuffled_df[args[1]] = shuffled_df[partial_col]
                        print(shuffled_df[args[1]].value_counts()) if verbose else None
                        shuffled_df = shuffled_df.drop(columns=[partial_col])
                return shuffled_df.loc[df.index]
            
            
#Replace a few values
def household_replace_transformer(household_parsed_df):
    ''' Funcion que transforma los valores en la encuesta Siuben reportados en valores enteros 
    a una interpretación booleana. Unicamente afecta la variable cs8comerprimerov2
    '''
    household_rename_map = {
            'cs8comerprimerov2': {
                    1: True,
                    2: False,
            }
    }
    household_replace_transformer = ReplaceTransformer(household_rename_map)
    household_rt_out = household_replace_transformer.transform(household_parsed_df)
    return    household_rt_out


def household_name_transfomrer(household_rt_out):
    '''Replace the name of the variables ans uses a more friendly reading name.
    '''
    # household
    household_names_map = {
            'nivelpobreza' : 'icv_cat',
            'icv' : 'icv_score',
            'cs8comerprimerov2': 'cep',
            'cs1noformularion': 'household_id',
            'cs_longitudcoordenadav250': 'lon',
            'cs_latitudcoordenadav250': 'lat',
            'cs1provinciav2': 'province_code',
            'cs1municipiov2': 'municipality_code',
            'cs1barriov3': 'neighborhood_code',
            
            'cs1seccionv2': 'section_code',
            'cs1distritomunicipalv2':'distmun_code'
            
            }
    household_keep_features = ['superate']
    household_name_transfomrer = NameTransformer(household_names_map, keep_features=household_keep_features)
    household_df = household_name_transfomrer.transform(household_rt_out)
    return    household_df


def create_municipality_neighborhood_keys(household_df):
    ''' Creates muncipality_key and neighborhood_key variables using 
    privince_code + municipality_code 
    privince_code + municipality_code + distmun_code + section_code and
    neighborhood_code.
    '''
    geo_assign_map = {
            'municipality_key': lambda df: df.province_code.astype(str).str.zfill(
                    2).str.cat(df.municipality_code.astype(str).str.zfill(2)),
            
            'neighborhood_key': lambda df: df.province_code.astype(str).str.zfill(
                    2).str.cat(df.municipality_code.astype(str).str.zfill( 
                    2).str.cat(df.distmun_code.astype(str).str.zfill(
                    2).str.cat(df.section_code.astype(str).str.zfill(
                    2).str.cat(df.neighborhood_code.astype(str).str.zfill(3)))))
    }
    geo_assign_transformer = AssignTransformer(geo_assign_map)
    households_ast_out = geo_assign_transformer.transform(household_df)
    return households_ast_out


def bernoulli_imputation(households_ast_out):
    ''' Existe un gap entre muy grande entre las personas que reportaron pertenecer al programa SUPERATE a través
    de la encuesta SUPERATE y el número de receptores reportados en el padrón superate. Este gap se debe a que 
    las personas prefieren no reportar que se encuentran inscritos en el programa para recibir mas beneficios a través
    de los programas del módulo de cuidados (SIUBEN). Para llenar este gap y reportar correctamente el número de 
    personas que se encuentran en el padrón se ha construido una herramienta de imputación que determina la 
    probabilidad de una determinado miembro (persona que ha llenado el formulario SIUBEN ) de pertenecer al 
    programa superate. Para esto se ha creado una función bernoulli que nos permite agregar persona a nuestro
    pool de beneficiarios hasta que llenemos la cuota de beneficiarios para cada uno de los grupos de pobreza
    (icv_cat).
    '''
    assign_one_map = {
            'ones': lambda _: 1,
            # this supposes the CEP households who are reported on ESH are effectively 
            # CEP beneficiaries.
            'superate_or_cep': lambda df: df['cep'] | df['superate']
    }
    assign_one_transformer = AssignTransformer(assign_one_map)
    pre_impute_household_df = assign_one_transformer.transform(households_ast_out)
    
    # icv_prob = [icv1, icv2, icv3, icv4]
    base_beneficiaries_col = 'superate_or_cep'
    imputed_beneficiaries_col = 'superate_imputed'
    
    icv_srs = pre_impute_household_df.loc[pre_impute_household_df.superate_or_cep].groupby( 'icv_cat', dropna=False)['household_id'].count()
    icv_quotas = icv_srs.mul(1.35e6 / icv_srs.sum()).round().to_dict()
    
    bernoulli_shock_cep_tuples = [
            ('ones', 'selected', 
            lambda df, i=i, beneficiaries=icv_quotas[float(i)]: ((
                    beneficiaries - (df[base_beneficiaries_col] & df['icv_cat'].eq(
                            i)).sum()) / (
                    (~ df[base_beneficiaries_col]) & df['icv_cat'].eq(i)).sum()), 
            lambda df, i=i:(~ df[base_beneficiaries_col]) & df['icv_cat'].eq(
                    i)) for i in range(1, 5)]


    bernoulli_shock_superate_transformer = BernoulliShockWrapperTransformer(
            bernoulli_shock_cep_tuples, weight_col='ones')
    bss_out = bernoulli_shock_superate_transformer.transform(
            pre_impute_household_df)
    
    
    superate_assign_map = {
        imputed_beneficiaries_col: lambda df: (
                df['selected'].eq(0) & df['icv_cat'].notnull())
    }
    superate_assign_transformer = AssignTransformer(superate_assign_map)
    superate_at_out = superate_assign_transformer.transform(bss_out)
    return    superate_at_out


