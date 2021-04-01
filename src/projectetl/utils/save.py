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