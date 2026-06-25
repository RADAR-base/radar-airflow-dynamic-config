from typing import Union, Tuple
import numpy as np
from .generic import butterworth


def linear_filter(acc: np.ndarray, freq: float,
                  cutoff: Union[float, Tuple[float, float]] = 0.5,
                  order: int = 5) -> np.ndarray:
    """Find the non-gravitational acceleration using a high-pass filter.

    Filters input with a two-pass butterworth filter, returning the linear
    component of acceleration.
    To also de-noising using a bandpass filter, provide a both the
    low-pass and high-pass cutoff. e.g. (0.5, 10) to bandpass between
    0.5Hz and 10Hz

    Dispatch <np.ndarray[float]>

    Args:
        acc (np.ndarray[float]): A vector or array of acceleration values
            If multiple vectors are given in a 2d array, the 2nd dimension
            seperates vectors. i.e acc[m, n] where n is the dimension of
            acceleration.
        freq (float): Sampling frequency
        cutoff (float, Tuple[float, float]): Cut-off frequency. Default: 0.5
        order (int): Order of the filter. Default: 5

    Returns:
        np.ndarray[float]: Linear acceleration (above cut-off frequency)
    """
    shape = acc.shape
    ftype = 'highpass' if np.shape(cutoff) == () else 'bandpass'
    acc = acc.reshape(shape[0], 1 if len(shape) == 1 else shape[1])
    res = np.zeros(acc.shape)
    for i in range(res.shape[1]):
        res[:, i] = butterworth(acc[:, i], cutoff=cutoff, freq=freq,
                                order=order, ftype=ftype)
    return res.reshape(shape)

def gravity_filter(acc: np.ndarray, freq: float,
                   cutoff: float = 0.5, order: int = 5) -> np.ndarray:
    """Find gravitational acceleration using a low-pass filter.

    Filters acceleration with a two-pass Butterworth filter,
    returning the gravitational component

    Dispatch <np.ndarray[float]>

    Args:
        acc (np.ndarray[float]): A vector or array of acceleration values
            If multiple vectors are given in a 2d array, the 2nd dimension
            seperates vectors. i.e acc[m, n] where n is the dimension of
            acceleration.
        freq (float): Sampling frequency
        cutoff (float): Cut-off frequency (Hz). Default: 0.5
        order (int): Order of the filter. Default: 5

    Returns:
        np.ndarray[float]: Gravitational component of acceleration


    Dispatch <pd.DataFrame>

    Args:
        df (pd.DataFrame): Dataframe containing acceleration
        freq (float): Sampling frequency
        cutoff (float): Low-pass cut-off frequency. Default: 0.5
        order (int): Order of filter. Default: 5
        columns (List[str]): List of column names. Optional

    Returns:
        pd.DataFrame: DataFrame containing filtered columns

    """
    shape = acc.shape
    acc = acc.reshape(shape[0], 1 if len(shape) == 1 else shape[1])
    res = np.zeros(acc.shape)
    for i in range(res.shape[1]):
        res[:, i] = butterworth(acc[:, i], cutoff=cutoff, freq=freq,
                                order=order, ftype='lowpass')
    return res.reshape(shape)

def pitch(x, y, z):
    """Estimate angular pitch from gravitational acceleration

    Args:
        x, y, z (float, int, array-like): x, y, and z acceleration

    Returns:
        (float, int, array-like): pitch

    """
    return np.arctan2(-x, np.sqrt(y*y + z*z)) * 180/np.pi

def roll(y, z):
    """Estimate angular roll from gravitational acceleration.

    Args:
        y, z (float, int, array-like): y, and z acceleration

    Returns:
        (float, int, array-like): roll

    """
    return np.arctan2(y, z) * 180/np.pi

def magnitude(x: float, y: float, z: float) -> float:
    """ Magnitude of x, y, z acceleration √(x²+y²+z²)

    Dispatch <float>

    Args:
        x (float): X-axis of acceleration
        y (float): Y-axis of acceleration
        z (float): Z-axis of acceleration

    Returns:
        float: Magnitude of acceleration

    Returns:
        float: Magnitude of acceleration
    """
    return np.sqrt(x**2 + y**2 + z**2)