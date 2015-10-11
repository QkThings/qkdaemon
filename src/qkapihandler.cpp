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

#include "qkapihandler.h"
#include "qkconnect.h"
#include "qkcore.h"
#include "qknode.h"
#include "qkprotocol.h"

#include <QDebug>
#include <QJsonDocument>
#include <QJsonObject>
#include <QJsonValue>
#include <QJsonArray>
#include <QFile>
#include <QStringList>
#include <QRegularExpression>
#include <QSignalMapper>

QkAPIHandler::QkAPIHandler(QkConnectionManager *connManager, QObject *parent) :
    QObject(parent),
    m_connManager(connManager)
{
    m_realTimeEnabled = true;
    m_apiTreeRoot = 0;
    create();
    setupConnections();
}

void QkAPIHandler::setRealTime(bool enabled)
{
    m_realTimeEnabled = enabled;
}

void QkAPIHandler::handleJsonRpc(const QString &jsonRpc)
{
    QJsonParseError parseError;
    QJsonDocument doc = QJsonDocument::fromJson(jsonRpc.toUtf8(), &parseError);
    if(parseError.error != QJsonParseError::NoError)
    {
        qDebug() << "JSON parse error:" << parseError.errorString();
        return;
    }
    if(!doc.isObject())
    {
        qDebug() << "JSON document is not an object";
        return;
    }

    QJsonObject obj = doc.object();
    QVariantMap map = obj.toVariantMap();
    qDebug() << map;
    QString methodName = map["method"].toString();
    QVariantList params = map["params"].toList();
    if(params.count() == 0) {
        qDebug() << "JSON RPC cannot have zero parameters";
        return;
    }
    QString path = params.takeFirst().toString();

    QJsonDocument jsonDoc;
    jsonDoc.setObject(run(methodName, path, params));
    emit sendJson(jsonDoc);
}

QJsonObject QkAPIHandler::run(const QString &methodName, const QString &path, QVariantList params)
{
    QStringList resourceList = path.split('/', QString::SkipEmptyParts);
    APITreeItem *nextTreeItem, *curTreeItem = m_apiTreeRoot;

    QMap<QString,QVariant> parsedValues;

    qDebug() << "resourceList:" << resourceList;

    QString resource = resourceList.takeFirst();
    if(resource != curTreeItem->resource)
    {
        qDebug() << "The first RPC name must be \"qk\"," << resource << curTreeItem->resource;
        return rpc_error(RPCError_Qk, "The first resource name must be \"qk\".");
    }

    while(!resourceList.isEmpty())
    {
        resource = resourceList.takeFirst();
        nextTreeItem = 0;

        // Try to find a child with the same resource name
        foreach(APITreeItem *child, curTreeItem->children)
        {
//            qDebug() << child->resource << resource;
            if(child->resource == resource)
            {
                nextTreeItem = child;
                break;
            }
        }
        // Try to get a value from resource
        if(nextTreeItem == 0)
        {
            foreach(APITreeItem *child, curTreeItem->children)
            {
                qDebug() << child->resource << resource;
                if(child->resource.contains(QRegularExpression("\\{.*?\\}")))
                {
                    parsedValues.insert(child->resource, QVariant(resource));
                    qDebug() << "parsedValues" << parsedValues;
                    nextTreeItem = child;
                    break;
                }
            }
        }

        if(nextTreeItem != 0)
            curTreeItem = nextTreeItem;
        else
        {
            qDebug() << "Parsing error: resource not found";
            return rpc_error(RPCError_Qk, "Resource name not found: " + resource);
        }
    }

    // Check if requested method type is supported by resource
    if((methodName == "get" && (curTreeItem->attributes & GET) == 0) ||
       (methodName == "set" && (curTreeItem->attributes & SET) == 0))
    {
        return rpc_error(RPCError_Qk, "Unsupported method name");
    }

    QString rpcName = curTreeItem->rpcName;
    qDebug() << "call RPC:" << curTreeItem->rpcName;
    if(!m_rpcMapper.contains(rpcName))
    {
        qDebug() << "RPC mapper doesn't contain" << rpcName;
        return rpc_error(RPCError_Qk, "RPC mapper doesn't have " + rpcName + " (internal)");
    }

    RPCArgs rpcArgs;
    rpcArgs.path =  path.split('/', QString::SkipEmptyParts);
    rpcArgs.parsed = &parsedValues;
    rpcArgs.params = &params;

    RPC rpc = m_rpcMapper.value(rpcName);
    return (*this.*rpc)(&rpcArgs);
}


void QkAPIHandler::create()
{
    QFile file(":/api/api_desc.txt");

    if (!file.open(QIODevice::ReadOnly | QIODevice::Text))
    {
       qDebug() << "Unable to read qkapi.txt";
       return;
    }

    QTextStream in(&file);
    QString line;
    int level, maxLevel = 0;
    while(!in.atEnd())
    {
        line = in.readLine();
        level = line.count('-');
        if(level > maxLevel)
            maxLevel = level;
    }

    APITreeItem *treeItem, *parentItem;
    QVector<APITreeItem*> parentArray(maxLevel);
    in.seek(0);
    while(!in.atEnd())
    {
        line = in.readLine();

        if(line.at(0) == '#') // comment
            continue;

        QStringList cols = line.split(',');
        if(cols.count() != APITreeColumns)
        {
            qDebug() << "qkapi.txt: wrong number of columns:" << cols.count() << "should be:" << APITreeColumns;
            return;
        }

        QString resource = cols[0];
        QString attrStr = cols[1].remove(QRegularExpression("\\t|\\s"));
        QString rpcName = cols[2].remove(QRegularExpression("\\t|\\s"));

        level = resource.count('-');
        resource.remove('\\');
        resource.remove("-");
        resource.remove(QRegularExpression("\\t|\\s"));

        if(level == 0)
        {
            if(m_apiTreeRoot == 0)
            {
                treeItem = new APITreeItem(resource, rpcName);
                treeItem->setAttributesFromString(attrStr);
                m_apiTreeRoot = treeItem;
            }
            else
            {
                qDebug() << "ERROR: multiple items at level 0";
                return;
            }
        }
        else
        {
            parentItem = parentArray.at(level - 1);
//            qDebug() << QString().fill(' ', level) << resource << rpcName;
            if(parentItem == 0)
                qDebug() << "ERROR: parent is null";
            treeItem = new APITreeItem(resource, rpcName, parentItem);
            treeItem->setAttributesFromString(attrStr);
            parentItem->children.append(treeItem);
        }

        parentArray[level] = treeItem;
        qDebug() << QString().fill(' ', level) + treeItem->resource;
    }
    file.close();

    m_rpcMapper.insert("listQk",            &QkAPIHandler::rpc_listQk);
    m_rpcMapper.insert("listConns",         &QkAPIHandler::rpc_listConns);
    m_rpcMapper.insert("conn",              &QkAPIHandler::rpc_conn);
    m_rpcMapper.insert("listSubscriptions", &QkAPIHandler::rpc_listSubscriptions);
    m_rpcMapper.insert("subscribeData",     &QkAPIHandler::rpc_subscribeData);
    m_rpcMapper.insert("subscribeEvent",    &QkAPIHandler::rpc_subscribeEvent);
    m_rpcMapper.insert("subscribeDebug",    &QkAPIHandler::rpc_subscribeDebug);
    m_rpcMapper.insert("listCmds",          &QkAPIHandler::rpc_listCmds);
    m_rpcMapper.insert("search",            &QkAPIHandler::rpc_search);
    m_rpcMapper.insert("start",             &QkAPIHandler::rpc_start);
    m_rpcMapper.insert("stop",              &QkAPIHandler::rpc_stop);
    m_rpcMapper.insert("listNodes",         &QkAPIHandler::rpc_listNodes);
    m_rpcMapper.insert("node",              &QkAPIHandler::rpc_node);
    m_rpcMapper.insert("nodeComm",          &QkAPIHandler::rpc_comm);
    m_rpcMapper.insert("nodeDevice",        &QkAPIHandler::rpc_device);
    m_rpcMapper.insert("info",              &QkAPIHandler::rpc_info);
    m_rpcMapper.insert("listConfigs",       &QkAPIHandler::rpc_listConfigs);
    m_rpcMapper.insert("config",            &QkAPIHandler::rpc_config);
    m_rpcMapper.insert("samplingInfo",      &QkAPIHandler::rpc_samplingInfo);
    m_rpcMapper.insert("samplingFrequency", &QkAPIHandler::rpc_samplingFrequency);
    m_rpcMapper.insert("samplingMode",      &QkAPIHandler::rpc_samplingMode);
    m_rpcMapper.insert("data",              &QkAPIHandler::rpc_data);
    m_rpcMapper.insert("listEvents",        &QkAPIHandler::rpc_events);
    m_rpcMapper.insert("listActions",       &QkAPIHandler::rpc_actions);
    m_rpcMapper.insert("action",            &QkAPIHandler::rpc_action);
    m_rpcMapper.insert("update",            &QkAPIHandler::rpc_update);


}

QJsonObject QkAPIHandler::create_object_conn(int connID)
{
    int i;
    QJsonObject mainObj;
    QkConnection *conn;
    conn = m_connManager->connection(connID);

    if(conn == 0)
        return mainObj;

    QJsonArray paramsArray;
    QkConnection::Descriptor desc = conn->descriptor();
    //mainObj.insert("type", QJsonValue(QkConnection::typeToString(desc.type)));

//    for(i = 0; i < desc.params.count(); i++)
//        paramsArray.insert(i, QJsonValue(desc.params[i]));
//    mainObj.insert("params", QJsonValue(paramsArray));

    return mainObj;
}

QJsonObject QkAPIHandler::create_object_node(QkConnection *conn, int address, bool insertAddress)
{
    QkNode *node = conn->qk()->node(address);

    QJsonObject mainObj, commObj, deviceObj;

    if(node->comm() != 0)
    {
        commObj.insert("name", node->comm()->name());
    }
    mainObj.insert("comm", QJsonValue(commObj));

    if(node->device() != 0)
    {
        deviceObj.insert("name", node->device()->name());
    }
    mainObj.insert("device", QJsonValue(deviceObj));
    if(insertAddress)
        mainObj.insert("address", QJsonValue(address));

    return mainObj;
}

QJsonObject QkAPIHandler::create_object_samplingInfo(int address)
{
    QkConnection *conn = m_connManager->defaultConnection();
    QkDevice *device = conn->qk()->node(address)->device();

    QJsonObject mainObj, sampObj;

    QkDevice::SamplingInfo info = device->samplingInfo();

    sampObj.insert("frequency", QJsonValue(info.frequency));
    sampObj.insert("mode", QJsonValue(QkDevice::samplingModeString(info.mode)));
    sampObj.insert("triggerClock", QJsonValue(QkDevice::triggerClockString(info.triggerClock)));
    sampObj.insert("triggerScaler", QJsonValue(info.triggerScaler));
    sampObj.insert("N", QJsonValue(info.N));
    mainObj.insert("sampling", QJsonValue(sampObj));

    return mainObj;
}

QJsonObject QkAPIHandler::rpc_error(int code, const QString &message)
{
    qDebug() << "rpc_error";
    QJsonObject obj, valueObj, errObj;
    valueObj.insert("value", QJsonValue(255));
    errObj.insert("code", QJsonValue(code));
    errObj.insert("message", QJsonValue(message));
    valueObj.insert("error",errObj);
    obj.insert("result", QJsonValue(valueObj));
    return obj;
}

QJsonObject QkAPIHandler::rpc_result(int value)
{
    qDebug() << "rpc_result";
    QJsonObject obj, valueObj;
    valueObj.insert("value", QJsonValue(value));
    obj.insert("result", QJsonValue(valueObj));
    return obj;
}

QJsonObject QkAPIHandler::rpc_listQk(RPCArgs *args)
{
    qDebug() << "rpc_listQk";

    QJsonObject obj;
    obj.insert("conns", QJsonValue(rpc_listConns(args)));
    obj.insert("nodes", QJsonValue(rpc_listNodes(args)));
    return obj;
}

QJsonObject QkAPIHandler::rpc_listConns(RPCArgs *args)
{
    qDebug() << "rpc_listConns";

    int i,j;
    (void)args; // not used

    QJsonObject obj;
    QJsonArray connsArray;
    QJsonObject connObj;
    for(i = 0; i < m_connManager->connections().count(); i++)
    {
        connObj = create_object_conn(i);
        connsArray.insert(i, QJsonValue(connObj));
    }
    obj.insert("conns", QJsonValue(connsArray));
    return obj;
}

QJsonObject QkAPIHandler::rpc_conn(RPCArgs *args)
{
    qDebug() << "rpc_conn";
    qDebug() << *(args->parsed);
    qDebug() << *(args->params);

    bool ok;
    QString parsedValueStr;
    parsedValueStr = args->parsed->value("{conn_id}").toString();
    int connID = parsedValueStr.toInt(&ok);
    if(!ok)
        return rpc_error(RPCError_Qk, tr("Invalid connection ID: ") + parsedValueStr);

    QkConnection *conn = m_connManager->connection(connID);
    if(conn == 0)
        return rpc_error(RPCError_Qk, tr("Invalid connection ID: ") + parsedValueStr);

    QJsonObject obj;
    QJsonObject connObj;
    connObj = create_object_conn(connID);
    obj.insert("conn" + QString::number(connID), QJsonValue(connObj));

    return obj;
}

QJsonObject QkAPIHandler::rpc_listNodes(RPCArgs *args)
{
    qDebug() << "rpc_listNodes";
    qDebug() << *(args->parsed);
    qDebug() << *(args->params);

    QJsonObject mainObj, errObj;

    if(!check_rpc_connection(&errObj))
        return errObj;

    int conn_id = args->parsed->value("{conn_id}").toInt();
    QkConnection *conn = m_connManager->connections()[conn_id];

    QJsonObject nodesObj;
    QJsonObject nodeObj;

    QList<int> addressList = conn->qk()->nodes().keys();

    foreach(int address, addressList)
    {
        nodeObj = create_object_node(conn, address, false);
        nodesObj.insert(QString::number(address), QJsonObject(nodeObj));
    }
    mainObj.insert("nodes", QJsonValue(nodesObj));

    return mainObj;
}

QJsonObject QkAPIHandler::rpc_node(RPCArgs *args)
{
    qDebug() << "rpc_node";
    qDebug() << *(args->parsed);
    qDebug() << *(args->params);

    QJsonObject mainObj, errObj, nodeObj;

    if(!check_rpc_connection(&errObj))
        return errObj;
    if(!validate_rpc_node(&errObj, args))
        return errObj;

    int conn_id = args->parsed->value("{conn_id}").toInt();
    QkConnection *conn = m_connManager->connections()[conn_id];

    int addr =  args->parsed->value("{addr}").toInt();
    nodeObj = create_object_node(conn, addr);
    mainObj.insert("node", QJsonValue(nodeObj));

    return mainObj;
}

QJsonObject QkAPIHandler::rpc_comm(RPCArgs *args)
{
    QJsonObject mainObj;
    return mainObj;
}

QJsonObject QkAPIHandler::rpc_device(RPCArgs *args)
{
    QJsonObject mainObj;
    return mainObj;
}

QJsonObject QkAPIHandler::rpc_info(RPCArgs *args)
{
    QJsonObject mainObj;
    return mainObj;
}

QJsonObject QkAPIHandler::rpc_listConfigs(RPCArgs *args)
{
    QJsonObject mainObj;
    return mainObj;
}

QJsonObject QkAPIHandler::rpc_config(RPCArgs *args)
{
    QJsonObject mainObj;
    return mainObj;
}

QJsonObject QkAPIHandler::rpc_samplingInfo(RPCArgs *args)
{
    QJsonObject mainObj, errObj;

    if(!check_rpc_connection(&errObj))
        return errObj;
    if(!validate_rpc_node(&errObj, args))
        return errObj;
    if(!validate_rpc_device(&errObj, args))
        return errObj;

    int addr =  args->parsed->value("{addr}").toInt();
    mainObj = create_object_samplingInfo(addr);
    return mainObj;
}

QJsonObject QkAPIHandler::rpc_samplingFrequency(RPCArgs *args)
{
    qDebug() << "rpc_samplingFrequency";
    qDebug() << *(args->parsed);
    qDebug() << *(args->params);

    //QMutexLocker locker(m_connManager->mutex());

    bool ok;
    QJsonObject errObj;

    if(!check_rpc_connection(&errObj))
        return errObj;
    if(!validate_rpc_node(&errObj, args))
        return errObj;
    if(!validate_rpc_device(&errObj, args))
        return errObj;

    int addr =  args->parsed->value("{addr}").toInt();

    if(args->params->count() != 1)
        return rpc_error(RPCError_Qk, tr("Invalid parameters"));

    QString sampFreqStr = args->params->at(0).toString();
    int sampFreq = sampFreqStr.toInt(&ok);
    if(!ok)
        return rpc_error(RPCError_Qk, tr("Invalid sampling frequency: ") + sampFreqStr);

    QkConnection *conn = m_connManager->defaultConnection();
    conn->qk()->node(addr)->device()->setSamplingFrequency(sampFreq);

    return rpc_result(0);
}

QJsonObject QkAPIHandler::rpc_samplingMode(RPCArgs *args)
{
    qDebug() << "rpc_samplingMode";
    qDebug() << *(args->parsed);
    qDebug() << *(args->params);

//    QMutexLocker locker(m_connManager->mutex());

    QJsonObject errObj;

    if(!check_rpc_connection(&errObj))
        return errObj;
    if(!validate_rpc_device(&errObj, args))
        return errObj;

    int addr =  args->parsed->value("{addr}").toInt();

    if(args->params->count() != 1)
        return rpc_error(RPCError_Qk, tr("Invalid parameters"));

    QString modeStr = args->params->at(0).toString();
    QkDevice::SamplingMode mode;
    if(modeStr == "continuous")
        mode = QkDevice::smContinuous;
    else if(modeStr == "triggered")
        mode = QkDevice::smTriggered;
    else
        return rpc_error(RPCError_Qk, tr("Unkown sampling mode:") + modeStr);

    QkConnection *conn = m_connManager->defaultConnection();
    conn->qk()->node(addr)->device()->setSamplingMode(mode);

    return rpc_result(0);
}

QJsonObject QkAPIHandler::rpc_data(RPCArgs *args)
{

}

QJsonObject QkAPIHandler::rpc_events(RPCArgs *args)
{

}

QJsonObject QkAPIHandler::rpc_actions(RPCArgs *args)
{

}

QJsonObject QkAPIHandler::rpc_action(RPCArgs *args)
{

}

QJsonObject QkAPIHandler::rpc_update(RPCArgs *args)
{
    qDebug() << "rpc_update";
    qDebug() << *(args->parsed);
    qDebug() << *(args->params);

    QJsonObject errObj;

    if(!check_rpc_connection(&errObj))
        return errObj;

    QkConnection *conn = m_connManager->defaultConnection();
    QkAck ack;

    if(args->path.contains("nodes"))
    {
        if(!validate_rpc_node(&errObj, args))
            return errObj;

        int addr =  args->parsed->value("{addr}").toInt();
        if(args->path.contains("comm"))
        {
            ack = QkAck::fromInt(conn->qk()->node(addr)->comm()->update());
        }
        else if(args->path.contains("device"))
        {
            if(!validate_rpc_device(&errObj, args))
                return errObj;
            ack = QkAck::fromInt(conn->qk()->node(addr)->device()->update());
        }
        conn->qk()->getNode(addr);
    }
    else if(args->path.contains("network"))
    {

    }
    else if(args->path.contains("gateway"))
    {

    }

    if(ack.result == QkAck::ACK_OK)
        return rpc_result(0);
    else
    {
        return rpc_error(ack.err, QkCore::errorMessage(ack.err));
    }
}

QJsonObject QkAPIHandler::rpc_listCmds(RPCArgs *args)
{
    qDebug() << "rpc_listCmds";
    qDebug() << *(args->parsed);
    qDebug() << *(args->params);

    QJsonObject obj;
    QJsonArray cmdsObj;

    int i;
    QStringList cmds;
    cmds << "search" << "start" << "stop";
    for(i = 0; i < cmds.count(); i++)
        cmdsObj.insert(i, QJsonValue(cmds.at(i)));

    obj.insert("cmds", cmdsObj);

    return obj;
}

QJsonObject QkAPIHandler::rpc_search(RPCArgs *args)
{
    QJsonObject errObj;

    if(!check_rpc_connection(&errObj))
        return errObj;

    QkConnection *defaultConn = m_connManager->defaultConnection();
    QkAck ack = QkAck::fromInt(defaultConn->qk()->search());
    if(ack.result == QkAck::ACK_OK)
        return rpc_result(0);
    else
        return rpc_error(ack.err, QkCore::errorMessage(ack.err));
}

QJsonObject QkAPIHandler::rpc_start(RPCArgs *args)
{
    QJsonObject errObj;

    if(!check_rpc_connection(&errObj))
        return errObj;

//    QkConnection *defaultConn = m_connManager->defaultConnection();
    int conn_id = args->parsed->value("{conn_id}").toInt();
    QkConnection *conn = m_connManager->connections()[conn_id];

    QkAck ack = QkAck::fromInt(conn->qk()->start());
    if(ack.result == QkAck::ACK_OK)
        return rpc_result(0);
    else
        return rpc_error(ack.err, QkCore::errorMessage(ack.err));
}

QJsonObject QkAPIHandler::rpc_stop(RPCArgs *args)
{
    QJsonObject errObj;

    if(!check_rpc_connection(&errObj))
        return errObj;

//    QkConnection *defaultConn = m_connManager->defaultConnection();
    int conn_id = args->parsed->value("{conn_id}").toInt();
    QkConnection *conn = m_connManager->connections()[conn_id];

    QkAck ack = QkAck::fromInt(conn->qk()->stop());
    if(ack.result == QkAck::ACK_OK)
        return rpc_result(0);
    else
        return rpc_error(ack.err, QkCore::errorMessage(ack.err));
}

QJsonObject QkAPIHandler::rpc_listSubscriptions(RPCArgs *args)
{
    qDebug() << "rpc_listSubscriptions";
    qDebug() << *(args->parsed);
    qDebug() << *(args->params);

    QJsonObject obj;
    QJsonArray subs;

    int i;
    QStringList subNames;
    subNames << "data";
    for(i = 0; i < subNames.count(); i++)
        subs.insert(i, QJsonValue(subNames.at(i)));

    obj.insert("subscriptions", subs);

    return obj;
}

QJsonObject QkAPIHandler::rpc_subscribeData(RPCArgs *args)
{
    qDebug() << __FUNCTION__;
    qDebug() << *(args->parsed);
    qDebug() << *(args->params);

    bool subscribe = args->params->at(0).toBool();
    if(subscribe)
        m_subscribedNames.insert("data");
    else
        m_subscribedNames.remove("data");

    return rpc_result(0);
}

QJsonObject QkAPIHandler::rpc_subscribeEvent(RPCArgs *args)
{
    qDebug() << __FUNCTION__;
    qDebug() << *(args->parsed);
    qDebug() << *(args->params);

    bool subscribe = args->params->at(0).toBool();
    if(subscribe)
        m_subscribedNames.insert("event");
    else
        m_subscribedNames.remove("event");

    return rpc_result(0);
}

QJsonObject QkAPIHandler::rpc_subscribeDebug(RPCArgs *args)
{
    qDebug() << __FUNCTION__;
    qDebug() << *(args->parsed);
    qDebug() << *(args->params);

    bool subscribe = args->params->at(0).toBool();
    if(subscribe)
        m_subscribedNames.insert("debug");
    else
        m_subscribedNames.remove("debug");

    return rpc_result(0);
}
#include "qkdevice.h"
QJsonObject QkAPIHandler::rpc_rt_data(RPCArgsRT *args)
{
    qDebug() << __FUNCTION__;

    QJsonObject obj;

    QkDevice::DataArray *dataArray = args->dataArray;

    QJsonObject dataObj, valueObj;
    QJsonArray valuesArray;

    dataObj.insert("conn_id", QJsonValue(args->connID));
    dataObj.insert("address", QJsonValue(args->address));
    int i;
    for(i = 0; i < dataArray->count(); i++)
    {
        valueObj.insert("label", QJsonValue((*dataArray)[i].label()));
        valueObj.insert("value", QJsonValue((*dataArray)[i].value()));
        valuesArray.insert(i, QJsonValue(valueObj));
    }
    dataObj.insert("values", QJsonValue(valuesArray));
    obj.insert("data", QJsonValue(dataObj));

    return obj;
}

QJsonObject QkAPIHandler::rpc_rt_event(RPCArgsRT *args)
{
    qDebug() << "rpc_rt_event";

    QJsonObject obj;

    QkDevice::Event *event = args->event;

    QJsonObject eventObj;
    QJsonArray argsArray;

    eventObj.insert("conn_id", QJsonValue(args->connID));
    eventObj.insert("address", QJsonValue(args->address));
    eventObj.insert("label", QJsonValue(event->label()));
    eventObj.insert("message", QJsonValue(event->message()));

    int i;
    for(i = 0; i < event->args().count(); i++)
    {
        argsArray.insert(i, QJsonValue(event->args().at(i)));
    }
    eventObj.insert("args", QJsonValue(argsArray));
    obj.insert("event", QJsonValue(eventObj));

    return obj;
}

QJsonObject QkAPIHandler::rpc_rt_debug(RPCArgsRT *args)
{
    qDebug() << "rpc_rt_debug";

    QJsonObject obj;

    QJsonObject debugObj;

    debugObj.insert("conn_id", QJsonValue(args->connID));
    debugObj.insert("address", QJsonValue(args->address));
    debugObj.insert("message", QJsonValue(args->text));

    obj.insert("debug", QJsonValue(debugObj));

    return obj;
}


bool QkAPIHandler::check_rpc_connection(QJsonObject *errorObj)
{
    if(m_connManager->defaultConnection() == 0)
    {
        *errorObj = rpc_error(RPCError_Qk, tr("Any connection available"));
        return false;
    }
    return true;
}

bool QkAPIHandler::check_rpc_connection(QJsonObject *errorObj, int connID)
{
    if(m_connManager->connection(connID) == 0)
    {
        *errorObj = rpc_error(RPCError_Qk, tr("Invalid connID: ") + QString::number(connID));
        return false;
    }

    return true;
}

bool QkAPIHandler::validate_rpc_node(QJsonObject *errorObj, RPCArgs *args)
{
    bool ok;
    QString addrStr = args->parsed->value("{addr}").toString();
    int addr = addrStr.toInt(&ok);
    if(!ok)
    {
        *errorObj = rpc_error(RPCError_Qk, tr("Invalid address: ") + addrStr);
        return false;
    }

    QkConnection *conn = m_connManager->defaultConnection();
    if(conn->qk()->node(addr) == 0)
    {
        *errorObj = rpc_error(RPCError_Qk, tr("Node not found (address=") + QString().sprintf("%04X)", addr));
        return false;
    }

    return true;
}

bool QkAPIHandler::validate_rpc_device(QJsonObject *errorObj, RPCArgs *args)
{
    int addr = args->parsed->value("{addr}").toInt();
    qDebug() << __FUNCTION__ << addr;

    QkConnection *conn = m_connManager->defaultConnection();

    if(!validate_rpc_node(errorObj, args))
        return false;

    if(conn->qk()->node(addr)->device() == 0)
    {
        *errorObj = rpc_error(RPCError_Qk, tr("Device not found"));
        return false;
    }
    return true;
}

void QkAPIHandler::slotDataReceived(int address, QkDevice::DataArray dataArray)
{
    qDebug() << "handle data received from" << address << "sender" << sender();

    if(!m_realTimeEnabled || !m_subscribedNames.contains("data"))
        return;

    QkProtocol *protocol = dynamic_cast<QkProtocol*>(sender());
    QkConnection *conn = protocol->qk()->connection();

    QJsonDocument jsonDoc;
    RPCArgsRT args;
    args.conn = conn;
    args.connID = conn->id();
    args.address = address;
    args.dataArray = &dataArray;

    jsonDoc.setObject(rpc_rt_data(&args));
    emit sendJson(jsonDoc);
}

void QkAPIHandler::slotEventReceived(int address, QkDevice::Event event)
{
    qDebug() << "handle event received from" << address;

    if(!m_realTimeEnabled || !m_subscribedNames.contains("event"))
        return;


    QkProtocol *protocol = dynamic_cast<QkProtocol*>(sender());
    QkConnection *conn = protocol->qk()->connection();
//    QkDevice *device = protocol->qk()->node(address)->device();

//    QQueue<QkDevice::Event> *events = device->eventLog();

    QJsonDocument jsonDoc;
    RPCArgsRT args;

    args.conn = conn;
    args.connID = conn->id();
    args.address = address;
    args.event = &event;

    jsonDoc.setObject(rpc_rt_event(&args));
    emit sendJson(jsonDoc);


//    while(events->count() > 0)
//    {
//        QkDevice::Event event = events->dequeue();
//        args.conn = conn;
//        args.connID = conn->id();
//        args.address = address;
//        args.event = &event;

//        jsonDoc.setObject(rpc_rt_event(&args));
//        emit sendJson(jsonDoc);
//    }
}

void QkAPIHandler::slotDebugReceived(int address, QString str)
{
    qDebug() << "handle debug received from" << address;

    if(!m_realTimeEnabled || !m_subscribedNames.contains("debug"))
        return;


    QkProtocol *protocol = dynamic_cast<QkProtocol*>(sender());
    QkConnection *conn = protocol->qk()->connection();

//    int connID = m_connManager->connectionID(conn);

    QJsonDocument jsonDoc;
    RPCArgsRT args;
    args.conn = conn;
    args.connID = conn->id();
    args.address = address;
    args.text = str;

    jsonDoc.setObject(rpc_rt_debug(&args));
    emit sendJson(jsonDoc);
}

void QkAPIHandler::slotConnectionAdded(QkConnection *conn)
{
    QkProtocol *protocol = conn->qk()->protocol();
    connect(protocol, SIGNAL(dataReceived(int, QkDevice::DataArray)),
            this, SLOT(slotDataReceived(int, QkDevice::DataArray)));
    connect(protocol, SIGNAL(eventReceived(int, QkDevice::Event)),
            this, SLOT(slotEventReceived(int, QkDevice::Event)));
    connect(protocol, SIGNAL(debugReceived(int,QString)),
            this, SLOT(slotDebugReceived(int,QString)));
}

void QkAPIHandler::slotConnectionRemoved(QkConnection *conn)
{
    QkProtocol *protocol = conn->qk()->protocol();
    disconnect(protocol, SIGNAL(dataReceived(int, QkDevice::DataArray)),
               this, SLOT(slotDataReceived(int, QkDevice::DataArray)));
    disconnect(protocol, SIGNAL(eventReceived(int, QkDevice::Event)),
               this, SLOT(slotEventReceived(int, QkDevice::Event)));
    disconnect(protocol, SIGNAL(debugReceived(int,QString)),
               this, SLOT(slotDebugReceived(int,QString)));
}

void QkAPIHandler::setupConnections()
{
    connect(m_connManager, SIGNAL(connectionAdded(QkConnection*)),
            this, SLOT(slotConnectionAdded(QkConnection*)));
    connect(m_connManager, SIGNAL(connectionRemoved(QkConnection*)),
            this, SLOT(slotConnectionRemoved(QkConnection*)));

    int id;
    QList<QkConnection*> conns = m_connManager->connections();
    for(id = 0; id < conns.count(); id++)
    {
        slotConnectionAdded(conns[id]);
    }
}

QkAPIHandler::APITreeItem::APITreeItem(QString resource, QString rpcName, APITreeItem *parent)
{
    this->resource = resource;
    this->attributes = 0;
    this->rpcName = rpcName;
    this->parent = parent;
}

void QkAPIHandler::APITreeItem::setAttributesFromString(const QString &attrStr)
{
    this->attributes = 0;
    if(attrStr.contains("get"))
        this->attributes |= GET;
    if(attrStr.contains("set"))
        this->attributes |= SET;
}
