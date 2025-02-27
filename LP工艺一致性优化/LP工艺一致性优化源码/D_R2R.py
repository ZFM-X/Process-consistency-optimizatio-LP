# !/usr/bin/env python
# -*-coding:utf-8 -*-
# File       : D_R2R.py
# Time       ：2024/6/5 18:46
# Author     ：ZFM
# version    ：python 3.6


# ------------------------------------------------- R2R类 ------------------------------------------------------------ #
class R2R(object):
    def __init__(self, YK, DLV_in, A_t, A_r, W, deta1, Y_LSL_warning, Y_USL_warning, YK_target, CK_1, max_change,
                        DLV_out_LSL, DLV_out_USL):
        self.YK = YK
        
        
    def R2R_initial(self):
        """
        R2R初始化
        """
        self.CK = self.YK - self.A * self.DLV_in
        return CK


# ------------------------------------------------ 时间调整 ----------------------------------------------------------- #
def R2R_time(YK, DLV_in, A_t, W, Y_LSL_warning, Y_USL_warning, YK_target, CK_1, max_change, DLV_out_LSL, DLV_out_USL):
    """
    计算R2R控制逻辑
    1.YK：膜厚需要控制上线限
    2.ROC:调整步长需要上下限
    3.DLV_out：实际输出需要上下限
    :param YK: 膜厚
    :param DLV_in: 输入：时间
    :param A_t: 时间系数
    :param Y_LSL_warning: 膜厚报警下限
    :param Y_USL_warning: 膜厚报警上限
    :param CK_1:
    :param max_change: 单步最大调整量
    :param DLV_out_LSL: 输出上限
    :param DLV_out_USL: 输出下限
    :return:
    """

    # 膜厚超过范围，不调整，报警推送
    global deta1, YK_pred
    if YK >= Y_USL_warning or YK <= Y_LSL_warning:
        DLV_out = DLV_in
        CK = CK_1

    else:
        # 根据实际膜厚，权重，计算CK
        CK = W * (YK - DLV_in * A_t) + (1 - W) * CK_1

        # 根据逻辑控制和实际膜厚，前馈1，前馈2，计算输出
        Xout = (YK_target - CK) / A_t

        # 根据计算输出Xout，输入DLV_in和调整步长之间的关系，计算ROC
        if Xout - DLV_in > max_change:
            ROC = DLV_in + max_change
        elif Xout - DLV_in < -max_change:
            ROC = DLV_in - max_change
        else:
            ROC = Xout

        # 根据，计算输出Xout，计算实际输出DLV_out
        if ROC > DLV_out_USL:
            DLV_out = DLV_out_USL
        elif ROC < DLV_out_LSL:
            DLV_out = DLV_out_LSL
        else:
            DLV_out = ROC

        DLV_out = round(DLV_out)  # 取整数

        deta1 = DLV_out - DLV_in
        YK_pred = DLV_out * A_t + CK

    return CK, DLV_out, deta1, YK_pred


# ----------------------------------------------- 2-5温区调整 ---------------------------------------------------------- #
def R2R_Temp_first_step(YK, DLV_in, A_t, A_r, W, deta1, Y_LSL_warning, Y_USL_warning, YK_target, CK_1, max_change,
                        DLV_out_LSL, DLV_out_USL):
    """
    计算R2R控制逻辑
    1.YK：膜厚需要控制上线限
    2.ROC:调整步长需要上下限
    3.DLV_out：实际输出需要上下限
    :param YK: 膜厚
    :param DLV_in: 输入：温度
    :param A_t: 时间系数
    :param A_r: 主温区系数2-5
    :param W: 权重
    :param deta1: 时间调整
    :param Y_LSL_warning: 膜厚报警下限
    :param Y_USL_warning: 膜厚报警上限
    :param CK_1:
    :param max_change: 单步最大调整量
    :param DLV_out_LSL: 输出上限
    :param DLV_out_USL: 输出下限
    :return:
    """
    # 膜厚超过范围，不调整，报警推送
    global deta2, YK_pred
    if YK >= Y_USL_warning or YK <= Y_LSL_warning:
        DLV_out = DLV_in
        CK = CK_1

    else:
        # 根据实际膜厚，权重，计算CK
        CK = W * (YK - DLV_in * A_r) + (1 - W) * CK_1

        # 根据逻辑控制和实际膜厚，前馈1，前馈2，计算输出
        Xout = (YK_target - CK - deta1 * A_t) / A_r

        # 根据计算输出Xout，输入DLV_in和调整步长之间的关系，计算ROC
        if Xout - DLV_in > max_change:
            ROC = DLV_in + max_change
        elif Xout - DLV_in < -max_change:
            ROC = DLV_in - max_change
        else:
            ROC = Xout

        # 根据，计算输出Xout，计算实际输出DLV_out
        if ROC > DLV_out_USL:
            DLV_out = DLV_out_USL
        elif ROC < DLV_out_LSL:
            DLV_out = DLV_out_LSL
        else:
            DLV_out = ROC

        DLV_out = round(DLV_out)  # 取整数

        deta2 = DLV_out - DLV_in
        YK_pred = DLV_out * A_r + deta1 * A_t + CK

    return CK, DLV_out, deta2, YK_pred


# ----------------------------------------------- 1-6温区调整 ---------------------------------------------------------- #
def R2R_Temp_second_step(YK, DLV_in, A_t, A_r, A_m, W, deta1, deta2, Y_LSL_warning, Y_USL_warning, YK_target, CK_1,
                         max_change, DLV_out_LSL, DLV_out_USL):
    """
    计算R2R控制逻辑
    1.YK：膜厚需要控制上线限
    2.ROC:调整步长需要上下限
    3.DLV_out：实际输出需要上下限
    :param YK: 膜厚
    :param DLV_in: 输入：时间/温度
    :param A_t: 时间系数
    :param A_r: 主温区温度系数2-5
    :param A_m: 时间系数
    :param A_t: 时间系数
    :param A_t: 时间系数
    :param Y_LSL_warning: 膜厚报警下限
    :param Y_USL_warning: 膜厚报警上限
    :param CK_1:
    :param max_change: 单步最大调整量
    :param DLV_out_LSL: 输出上限
    :param DLV_out_USL: 输出下限
    :return:
    """
    # 膜厚超过范围，不调整，报警推送
    global YK_pred
    if YK >= Y_USL_warning or YK <= Y_LSL_warning:
        DLV_out = DLV_in
        CK = CK_1

    else:
        # 根据实际膜厚，权重，计算CK
        CK = W * (YK - DLV_in * A_m) + (1 - W) * CK_1

        # 根据逻辑控制和实际膜厚，前馈1，前馈2，计算输出
        Xout = (YK_target - CK - A_t * deta1 - A_r * deta2) / A_m

        # 根据计算输出Xout，输入DLV_in和调整步长之间的关系，计算ROC
        if Xout - DLV_in > max_change:
            ROC = DLV_in + max_change
        elif Xout - DLV_in < -max_change:
            ROC = DLV_in - max_change
        else:
            ROC = Xout

        # 根据，计算输出Xout，计算实际输出DLV_out
        if ROC > DLV_out_USL:
            DLV_out = DLV_out_USL
        elif ROC < DLV_out_LSL:
            DLV_out = DLV_out_LSL
        else:
            DLV_out = ROC

        DLV_out = round(DLV_out)  # 取整数
        YK_pred = DLV_out * A_m + deta1 * A_t + deta2 * A_r + CK

    return CK, DLV_out, YK_pred
