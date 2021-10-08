from projectetl.utils.config import S3_BUCKET_NAME, S3_KEY_DICT, \
    S3_SUBPRODUCTS_DIR
import boto3
import pickle as pk
from dotenv import load_dotenv


def protected_save(save_func, *args, ask_before_save=True):
    """
    Wrapper for saving outputs
    Parameters
    ----------
    save_func : callable
        It saves the output. Its first argument needs to be 
        `to_save_object`.
    args : optional,
        Positional argumets to be passed to `save_func`. The second 
        argument (if any) is assumed to be the path where the file will
        be stored.
    ask_before_save : bool or None.
        If True, it asks for input to confirm saving. If False, it does 
        not ask for confirmation. If None it will not save the output.
    """
    if ask_before_save is None: 
        print('skip saving')
        return None
    save = True
    
    print(f'trying to save at: {args[1]}') if len(args) > 1 else None
    if ask_before_save:
        save &= (input('confirm saving? (y/n)') == 'y')
    
    if save:
        print('saving')
        save_func(*args)
    else:
        print('not saved')

# could go on covidsource
def save_s3_data(df,
                 key,
                 bucket_name):
    """
    Loads data DataFrame from s3 bucket

    Parameters
    ----------
    df : DataFrame
    key : str,
        AWS key to be used on bucket to find the target file
    profile_name : str ,
        AWS profile name.
    bucket_name : str ,
        AWS bucket name.

    columnnames_to_lower : bool,
        specifies if column names need to be converted to lower.
    load_func : callable,
        it's unique argument needs to be `path`.
    """
    # instance s3 connection
    s3_profile_name = os.getenv('AWS_PROFILE_NAME')
    boto3.setup_default_session(profile_name=s3_profile_name)
    s3_client = boto3.client('s3')

    # save file
    response = s3_client.put_object(
        Body=pk.dumps(df),
        Bucket=bucket_name,
        Key=key)

    http_status_code = response['ResponseMetadata']['HTTPStatusCode']
    if http_status_code != 200:
        raise ValueError(textwrap.fill(
            f'Save process did not succeed: {response}'
        ))


def save_s3_subproduct_sv(
    obj, path, bucket_name=S3_BUCKET_NAME,
    subproducts_dir=S3_SUBPRODUCTS_DIR
):
    """
    Save object to El Salvador subproducts AWS S3 directory via
    `save_s3_data` function.

    Parameters
    ----------
    obj
        Object to save.
    path : str
        Correspond to the path from the subproducts directory for saving
        `obj`.
    profile_name : str ,
        AWS profile name.
    bucket_name : str ,
        AWS bucket name.
    """
    s3_profile_name = os.getenv('AWS_PROFILE_NAME')
    return save_s3_data(obj, f'{subproducts_dir}/{path}', s3_profile_name,
                        bucket_name)
