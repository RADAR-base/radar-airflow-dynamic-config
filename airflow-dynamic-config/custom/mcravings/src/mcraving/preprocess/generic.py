from typing import Union, Tuple
import numpy as np
from scipy import signal

def butterworth(arr: np.ndarray, cutoff: Union[float, Tuple[float, float]],
                freq: float, order: int = 5,
                ftype: str = 'highpass') -> np.ndarray:
    """Butterworth filters the array through a highpass butterworth filter.

    Args:
        arr (list or numpy.array): The timeseries array
            on which the filter is applied
        cutoff (scalar or len-2 sequence): The critical frequencies for the
            Butterworth filter. The point at which the gain drops to 1/sqrt(2)
            of the passband. Must be length-2 sequence for a bandpass filter
            giving [low, high]. Or else the scalar cutoff frequency for either
            a low-pass' or 'highpass' filter.
        freq (float): The frequency (Hz) of the input array
        order (int): The order of the filter.
        ftype (str - {'highpass', 'lowpass', 'bandpass'}):
            The filter type. Default is 'highpass'

    Returns:
        np.array: Filtered input signal
    """
    nyq = 0.5 * freq
    if np.size(cutoff) == 1:
        Wn = cutoff / nyq
    else:
        Wn = [c / nyq for c in cutoff]
    b, a = signal.butter(order, Wn, ftype)
    return signal.filtfilt(b, a, arr)