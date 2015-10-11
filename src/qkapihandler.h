/*
 * QkThings LICENSE
 * The open source framework and modular platform for smart devices.
 * Copyright (C) 2014 <http://qkthings.com>
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef QKAPIHANDLER_H
#define QKAPIHANDLER_H

#include <QObject>
#include <QMap>
#include <QSet>
#include <QStringList>
#include "qkcore.h"

class QkConnectionManager;
class QkConnection;


class QkAPIHandler : public QObject
{
    Q_OBJECT
public:
    explicit QkAPIHandler(QkConnectionManager *connManager, QObject *parent = 0);
    
    void setRealTime(bool enabled);

signals:
    void sendJson(const QJsonDocument &doc);
    
public slots:
    void handleJsonRpc(const QString &jsonRpc);

private slots:
    void slotConnectionAdded(QkConnection *conn);
    void slotConnectionRemoved(QkConnection *conn);
    void slotDataReceived(int address, QkDevice::DataArray dataArray);
    void slotEventReceived(int address, QkDevice::Event event);
    void slotDebugReceived(int address, QString str);

private:
    enum Constants
    {
        APITreeColumns = 3
    };
    enum MethodAttribute
    {
        GET = (1<<0),
        SET = (1<<1)
    };
    typedef MethodAttribute MethodType;

    enum RPCError
    {
        RPCError_ParseError = -32700,
        RPCError_MethodNotFound = -32601,
        RPCError_InvalidParams = -32602,
        RPCError_InternalError = -32603,
        RPCError_Qk = 123 //TODO add error codes
    };

    class RPCArgs
    {
    public:
        QStringList path;
        MethodType methodType;
        QMap<QString,QVariant> *parsed;
        QVariantList *params;
    };

    class RPCArgsRT
    {
    public:
        int connID;
        QkConnection *conn;
        int address;
        QkDevice::DataArray *dataArray;
        QkDevice::Event *event;
        QString text;
        int errCode, errArg;
    };

    typedef QJsonObject (QkAPIHandler::*RPC)(RPCArgs *args);

    class APITreeItem
    {
    public:
        APITreeItem(QString resource, QString rpcName, APITreeItem *parent = 0);

        QString resource;
        int attributes;
        QString rpcName;
        APITreeItem *parent;
        QList<APITreeItem*> children;

        void setAttributesFromString(const QString &attrStr);
    };


    void setupConnections();
    void create();
    QJsonObject run(const QString &methodName, const QString &path, QVariantList params);

    QJsonObject create_object_conn(int connID);
    QJsonObject create_object_node(QkConnection *conn, int address = 0, bool insertAddress = true);
    QJsonObject create_object_samplingInfo(int address);


    QJsonObject rpc_error(int code, const QString &message);
    QJsonObject rpc_result(int value);
    QJsonObject rpc_listQk(RPCArgs *args);
    QJsonObject rpc_listConns(RPCArgs *args);
    QJsonObject rpc_conn(RPCArgs *args);
    QJsonObject rpc_listSubscriptions(RPCArgs *args);
    QJsonObject rpc_subscribeData(RPCArgs *args);
    QJsonObject rpc_subscribeEvent(RPCArgs *args);
    QJsonObject rpc_subscribeDebug(RPCArgs *args);
    QJsonObject rpc_listCmds(RPCArgs *args);
    QJsonObject rpc_search(RPCArgs *args);
    QJsonObject rpc_start(RPCArgs *args);
    QJsonObject rpc_stop(RPCArgs *args);
    QJsonObject rpc_listNodes(RPCArgs *args);
    QJsonObject rpc_node(RPCArgs *args);
    QJsonObject rpc_comm(RPCArgs *args);
    QJsonObject rpc_device(RPCArgs *args);
    QJsonObject rpc_info(RPCArgs *args);
    QJsonObject rpc_listConfigs(RPCArgs *args);
    QJsonObject rpc_config(RPCArgs *args);
    QJsonObject rpc_samplingInfo(RPCArgs *args);
    QJsonObject rpc_samplingFrequency(RPCArgs *args);
    QJsonObject rpc_samplingMode(RPCArgs *args);
    QJsonObject rpc_data(RPCArgs *args);
    QJsonObject rpc_events(RPCArgs *args);
    QJsonObject rpc_actions(RPCArgs *args);
    QJsonObject rpc_action(RPCArgs *args);
    QJsonObject rpc_update(RPCArgs *args);


    // Real-time (sent when subscribed)
    QJsonObject rpc_rt_data(RPCArgsRT *args);
    QJsonObject rpc_rt_event(RPCArgsRT *args);
    QJsonObject rpc_rt_debug(RPCArgsRT *args);

    bool check_rpc_connection(QJsonObject *errorObj);
    bool check_rpc_connection(QJsonObject *errorObj, int connID);
    bool validate_rpc_node(QJsonObject *errorObj, RPCArgs *args);
    bool validate_rpc_device(QJsonObject *errorObj, RPCArgs *args);

    QkConnectionManager *m_connManager;
    APITreeItem *m_apiTreeRoot;
    QMap<QString,RPC> m_rpcMapper;

    QSet<QString> m_subscribedNames;

    bool m_realTimeEnabled;
};

#endif // QKAPIHANDLER_H
