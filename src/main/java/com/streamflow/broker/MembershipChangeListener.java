package com.streamflow.broker;

import com.streamflow.partition.MembershipChangeEvent;

public interface MembershipChangeListener {
    void onMembershipChange(MembershipChangeEvent event);
}