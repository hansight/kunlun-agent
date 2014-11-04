package com.hansight.kunlun.agent.collector.common.model;

import com.sun.jna.Native;
import com.sun.jna.platform.win32.WTypes;
import com.sun.jna.platform.win32.WinDef;
import com.sun.jna.platform.win32.WinNT;
import com.sun.jna.win32.StdCallLibrary;
import com.sun.jna.win32.W32APIOptions;

/**
 * Author:zhhui
 * DateTime:2014/8/6 17:32.
 */
public interface WinEvt extends StdCallLibrary {

    public static class EVT_HANDLE extends WinNT.HANDLE {

    }

    public static class EVT_HANDLEByReference extends WinNT.HANDLEByReference {

    }

    WinEvt INSTANCE = (WinEvt) Native.loadLibrary("wevtapi",
            WinEvt.class, W32APIOptions.UNICODE_OPTIONS);

    WinNT.BOOL EvtArchiveExportedLog(WinNT.HANDLE session, WinNT.LCID locate, WinNT.DWORD flag);

    WinNT.BOOL EvtCancel(WinNT.HANDLE session);

    WinNT.BOOL EvtClearLog(WinNT.HANDLE session, String path, String filePath, WinNT.DWORD flag);

    WinNT.BOOL EvtClose(WinNT.HANDLE session);

    WinNT.HANDLE EvtCreateBookmark(String bookmark);

    WinNT.HANDLE EvtCreateRenderContext(WinNT.DWORD number, String[] values, WinNT.DWORD flag);

    WinNT.BOOL EvtExportLog(WinNT.HANDLE session, String path, String logtoFile, WinNT.DWORD flag);

    WinNT.DWORD EvtFormatMessage(
            EVT_HANDLE PublisherMetadata,
            EVT_HANDLE Event,
            WinDef.DWORD MessageId,
            WinDef.DWORD ValueCount,
            String[] Values,
            WinDef.DWORD Flags,
            WinDef.DWORD BufferSize,
            WTypes.LPWSTR Buffer
    );

    EVT_HANDLE EvtQuery(EVT_HANDLE Session, String Path, String Query, int Flags);

    boolean EvtNext(
            EVT_HANDLE ResultSet,
            int EventArraySize,
            EVT_HANDLE[] EventArray,
            int Timeout,
            int Flags,
            WinDef.DWORD[] Returned
    );

    boolean EvtRender(
            EVT_HANDLE Context,
            EVT_HANDLE Fragment,
            int Flags,
            WinDef.DWORD BufferSize,
           byte [] Buffer,
            WinDef.DWORDByReference BufferUsed,
            WinDef.DWORDByReference PropertyCount
    );

    public static int EvtRenderEventValues = 0;
    public static int EvtRenderEventXml = 1;
    public static int EvtRenderBookmark = 2;
    public static int EvtQueryChannelPath = 0x1;
    public static int EvtQueryFilePath = 0x2;
    public static int EvtQueryForwardDirection = 0x100;
    public static int EvtQueryReverseDirection = 0x200;
    public static int EvtQueryTolerateQueryErrors = 0x1000;

}
