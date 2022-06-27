from coffea import processor
import hist
import awkward as ak
import numpy as np

def normalize(val):
    return ak.to_numpy(ak.fill_none(val, np.nan))

def flatten(val):
    return ak.to_numpy(ak.flatten(val))

class TestProcessor(processor.ProcessorABC):
    def __init__(self):
        self._histo = hist.Hist(
            hist.axis.StrCategory([], name='dataset', label="Dataset", growth=True),
            hist.axis.Regular(100, 0, 1000, name="pt", label="Muon $p_T$"),
            hist.storage.Weight()
        )

    @property
    def accumulator(self):
        return self._histo

    def process(self, events):
        out = self.accumulator

        out.fill(
            dataset=events.metadata["dataset"],
            pt=flatten(events.Muon.pt),
        )
        return out

    def postprocess(self, accumulator):
        return accumulator