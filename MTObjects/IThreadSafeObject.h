#pragma once

#include <set>
#include <vector>
#include <algorithm>
#include <memory>
#include <assert.h>

namespace MTObjects
{
using std::set;
using std::vector;
using std::shared_ptr;
using std::weak_ptr;

struct IThreadSafeObject
{
private:
	friend struct Cluster;
	const Cluster* cluster_ = nullptr;

public:
	virtual vector<IThreadSafeObject*>	IsDependentOn()													const = 0;
	virtual void						IsConstDependentOn(set<const	IThreadSafeObject*>& ref_dependencies)	const = 0;
};

struct Cluster : public std::enable_shared_from_this<Cluster>
{
	set<IThreadSafeObject*> objects_;
	//clusters, that this cluster is const-dependent on.
	set<const Cluster*> const_dependencies_clusters_;

	bool DependsOnOtherCluster(const Cluster* cluster) const
	{
		return const_dependencies_clusters_.end() != const_dependencies_clusters_.find(cluster);
	}
private:
	bool TryAddObject(IThreadSafeObject* obj, set<IThreadSafeObject*>& all_objects, set<const IThreadSafeObject*>& const_dependencies_)
	{
		assert(obj);
		const bool was_added = objects_.emplace(obj).second;
		if (was_added)
		{
			obj->cluster_ = this;

			all_objects.erase(obj);
			obj->IsConstDependentOn(const_dependencies_);

			auto dependencies = obj->IsDependentOn();
			for (IThreadSafeObject* dep_obj : dependencies)
			{
				TryAddObject(dep_obj, all_objects, const_dependencies_);
			}
		}
		else
		{
			assert(obj->cluster_ == this);
		}
		return was_added;
	}

public:
	static vector<shared_ptr<Cluster>> BuildClusters(set<IThreadSafeObject*> all_objects)
	{
		vector<shared_ptr<Cluster>> clusters;
		vector<set<const IThreadSafeObject*>> all_const_dependencies;
		while (!all_objects.empty())
		{
			auto cluster = std::make_shared<Cluster>();
			set<const IThreadSafeObject*> const_dependencies;
			const bool cluster_created = cluster->TryAddObject(*all_objects.begin(), all_objects, const_dependencies);
			assert(cluster_created);

			clusters.emplace_back(cluster);
			all_const_dependencies.emplace_back(const_dependencies);
		}

		//Note: now all objects has proper cluster set
		for (int i = 0; i < all_const_dependencies.size(); i++)
		{
			auto& cluster = clusters[i];
			for (const IThreadSafeObject* obj : all_const_dependencies[i])
			{
				if (obj->cluster_ != cluster.get())
				{
					cluster->const_dependencies_clusters_.emplace(obj->cluster_);
				}
			}
		}

		return clusters;
	}
};

struct GroupOfConcurrentClusters : public vector<shared_ptr<Cluster>>
{
	static vector<GroupOfConcurrentClusters> SplitIntoConcurrentGroups(const vector<shared_ptr<Cluster>>& all_clusters)
	{
		auto fits_into_group = [](const GroupOfConcurrentClusters& group, const Cluster* cluster) -> bool
		{
			return std::none_of(group.begin(), group.end(), [&](const Cluster* cluster_in_group)
			{
				return cluster_in_group->DependsOnOtherCluster(cluster) || cluster->DependsOnOtherCluster(cluster_in_group);
			});
		};

		vector<GroupOfConcurrentClusters> groups;
		groups.reserve(all_clusters.size());
		for (auto& cluster : all_clusters)
		{
			bool fits_in_existing_group = false;
			for (auto& group : groups)
			{
				if (fits_into_group(group, cluster.get()))
				{
					group.emplace_back(cluster);
					fits_in_existing_group = true;
					break;
				}
			}
			if (!fits_in_existing_group)
			{
				groups.emplace_back(GroupOfConcurrentClusters());
				groups.back().emplace_back(cluster);
			}
		}
	}
};
}