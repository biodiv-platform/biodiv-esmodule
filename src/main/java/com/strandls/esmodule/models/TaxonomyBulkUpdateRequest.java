package com.strandls.esmodule.models;

import java.util.List;

public class TaxonomyBulkUpdateRequest {
    private List<TaxonomyBulkUpdateData> updates;
    private List<Long> recoIds;

    public List<TaxonomyBulkUpdateData> getUpdates() { return updates; }
    public void setUpdates(List<TaxonomyBulkUpdateData> updates) { this.updates = updates; }

    public List<Long> getRecoIds() { return recoIds; }
    public void setRecoIds(List<Long> recoIds) { this.recoIds = recoIds; }
}